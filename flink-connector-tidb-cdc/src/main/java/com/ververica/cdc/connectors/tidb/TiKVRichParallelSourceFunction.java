/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.tidb;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.tidb.table.StartupMode;
import com.ververica.cdc.connectors.tidb.table.utils.TableKeyRangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * The source implementation for TiKV that read snapshot events first and then read the change
 * event.
 */
public class TiKVRichParallelSourceFunction<T> extends RichParallelSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TiKVRichParallelSourceFunction.class);
    private static final long SNAPSHOT_VERSION_EPOCH = -1L;
    private static final long STREAMING_VERSION_START_EPOCH = 0L;

    private final TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
    private final TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;
    private final TiConfiguration tiConf;
    private final StartupMode startupMode;
    private final String database;
    private final String tableName;
    private int delayStartSeconds;

    /** Task local variables. */
    private transient TiSession session = null;

    private transient Coprocessor.KeyRange keyRange = null;
    private transient CDCClient cdcClient = null;
    private transient SourceContext<T> sourceContext = null;
    private transient volatile long resolvedTs = -1L;
    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> prewrites = null;
    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commits = null;
    private transient BlockingQueue<Cdcpb.Event.Row> committedEvents = null;
    private transient OutputCollector<T> outputCollector;

    private transient boolean running = true;
    private transient ExecutorService executorService;

    /** offset state. */
    private transient ListState<Long> offsetState;

    private static final long CLOSE_TIMEOUT = 30L;

    private boolean isInitalized = false;

    private int retryCount = 3;
    private long tableId;
    public TiKVRichParallelSourceFunction(
            TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema,
            TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema,
            TiConfiguration tiConf,
            StartupMode startupMode,
            String database,
            String tableName,
            int delayStartSeconds) {
        this.snapshotEventDeserializationSchema = snapshotEventDeserializationSchema;
        this.changeEventDeserializationSchema = changeEventDeserializationSchema;
        this.tiConf = tiConf;
        this.startupMode = startupMode;
        this.database = database;
        this.tableName = tableName;
        this.delayStartSeconds = delayStartSeconds;
        LOG.info("TiKVRichParallelSourceFunction init database {} tableName {} delayStartSeconds {}",database,tableName,delayStartSeconds);
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);

        int delay_seconds = 120;
        Random random = new Random();
        int tm = Math.abs(random.nextInt()) % delay_seconds;
        Thread.sleep(tm * 1000);

        session = TiSession.create(tiConf);
        TiTableInfo tableInfo = session.getCatalog().getTable(database, tableName);
        if (tableInfo == null) {
            throw new RuntimeException(
                    String.format("Table %s.%s does not exist.", database, tableName));
        }

        LOG.info("======finish getTable {} {} {} seconds",database,tableName,tm);

        tableId = tableInfo.getId();
        keyRange =
                TableKeyRangeUtils.getTableKeyRange(
                        tableId,
                        getRuntimeContext().getNumberOfParallelSubtasks(),
                        getRuntimeContext().getIndexOfThisSubtask());
        cdcClient = new CDCClient(session, keyRange);
        prewrites = new TreeMap<>();
        commits = new TreeMap<>();
        // cdc event will lose if pull cdc event block when region split
        // use queue to separate read and write to ensure pull event unblock.
        // since sink jdbc is slow, 5000W queue size may be safe size.
        committedEvents = new LinkedBlockingQueue<>();
        outputCollector = new OutputCollector<>();
        if (!isInitalized){
            resolvedTs =
                    startupMode == StartupMode.INITIAL
                            ? SNAPSHOT_VERSION_EPOCH
                            : STREAMING_VERSION_START_EPOCH;
            LOG.info("open source function {} {} {} {}", startupMode, database, tableName, resolvedTs);
        }
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat(
                                "tidb-source-function-"
                                        + getRuntimeContext().getIndexOfThisSubtask())
                        .build();
        executorService = Executors.newSingleThreadExecutor(threadFactory);
    }

    @Override
    public void run(final SourceContext<T> ctx) throws Exception {
        sourceContext = ctx;
        outputCollector.context = sourceContext;

        int delay_seconds;
        String delay_minute_env = System.getenv("TASK_START_RANDOM_DELAY_MINUTES");
        if(delay_minute_env == null || delay_minute_env.isEmpty()) {
            delay_seconds = 120;
        } else {
            delay_seconds = Integer.parseInt(delay_minute_env) * 60;
        }

        int batchSize =  tiConf.getScanBatchSize();
        LOG.info("============batchSize {}",batchSize);

        if (startupMode == StartupMode.INITIAL && !isInitalized && resolvedTs <= 0) {
            synchronized (sourceContext.getCheckpointLock()) {
                LOG.info("wait for start readSnapshotEvents {} {}  delayStartSeconds {} ",database, tableName,delayStartSeconds);
                Random random = new Random();
                int tm = Math.abs(random.nextInt()) % delay_seconds;
                LOG.info("wait for start readSnapshotEvents {} {} {} delay_minute_env {} seconds ",database, tableName,delay_minute_env,tm);
                Thread.sleep(tm * 1000);
                readSnapshotEvents();
            }
        } else {
            LOG.info("Skip snapshot read {} {} {} {}", database, tableName, isInitalized, resolvedTs);
            if (!isInitalized) {
                resolvedTs = session.getTimestamp().getVersion();
            }
        }

        LOG.info("start read change events {} {}", database, tableName);
        cdcClient.start(resolvedTs);
        running = true;
        readChangeEvents();
    }

    private void handleRow(final Cdcpb.Event.Row row) {
        if (!TableKeyRangeUtils.isRecordKey(row.getKey().toByteArray())) {
            // Don't handle index key for now
            return;
        }
        LOG.debug("binlog record, {} {}  type: {}, data: {}", database, tableName, row.getType(), row);
        switch (row.getType()) {
            case COMMITTED:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case COMMIT:
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case PREWRITE:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                break;
            case ROLLBACK:
                prewrites.remove(RowKeyWithTs.ofStart(row));
                break;
            default:
                LOG.warn("Unsupported row type:" + row.getType());
        }
    }

    protected void readSnapshotEvents() throws Exception {
        LOG.info("read snapshot events {} {} {}", database, tableName, resolvedTs);
        int count = 0;
        try (KVClient scanClient = session.createKVClient()) {
            long startTs = session.getTimestamp().getVersion();
            ByteString start = keyRange.getStart();
            int scanCount = 0;
            while (true) {
                final List<Kvrpcpb.KvPair> segment =
                        scanClient.scan(start, keyRange.getEnd(), startTs);

                if (segment.isEmpty()) {
                    if(count == 0 && scanCount < retryCount){
                        LOG.info("readSnapshotEvents fetch line is zero {} {} retry {}",database, tableName,scanCount);
                        scanCount++;
                        Thread.sleep(3 * 1000);
                        continue;
                    }
                    resolvedTs = startTs;
                    LOG.info("finish snapshot events {} {} {} {} {}", database, tableName, tableId,resolvedTs,count);
                    break;
                }

                for (final Kvrpcpb.KvPair pair : segment) {
                    if (TableKeyRangeUtils.isRecordKey(pair.getKey().toByteArray())) {
                        count++;
                        snapshotEventDeserializationSchema.deserialize(pair, outputCollector);
                    }
                }

                start =
                        RowKey.toRawKey(segment.get(segment.size() - 1).getKey())
                                .next()
                                .toByteString();
                Thread.sleep(300);
            }
        }
    }

    protected void readChangeEvents() throws Exception {
        LOG.info("read change event from resolvedTs:{} {} {} ", database, tableName, resolvedTs);
        BlockingQueue<Exception> exceptionBlockingQueue = new LinkedBlockingQueue<>();
        // child thread to sink committed rows.
        executorService.execute(
                () -> {
                    while (running) {
                        try {
                            Cdcpb.Event.Row committedRow = committedEvents.take();
                            changeEventDeserializationSchema.deserialize(
                                    committedRow, outputCollector);
                        } catch (Exception e) {
                            exceptionBlockingQueue.add(e);
                        }
                    }
                });
        while (resolvedTs >= STREAMING_VERSION_START_EPOCH) {
            for (int i = 0; i < 1000; i++) {
                final Cdcpb.Event.Row row = cdcClient.get();
                if (row == null) {
                    break;
                }
                handleRow(row);
            }
            Exception exception = exceptionBlockingQueue.poll();
            if (exception != null) {
                throw new FlinkRuntimeException("committed row exception:" + exception, exception);
            }
            long curResolvedTs = cdcClient.getMaxResolvedTs();
            if(curResolvedTs == 0){
                LOG.info("read change event exception resolvedTs is zero {} {}", database, tableName);
                continue;
            }
            if (commits.size() > 0) {
                flushRows(curResolvedTs);
            }
            resolvedTs = curResolvedTs;
        }
    }

    protected void flushRows(final long timestamp) throws Exception {
        Preconditions.checkState(sourceContext != null, "sourceContext shouldn't be null");
        synchronized (sourceContext) {
            while (!commits.isEmpty() && commits.firstKey().timestamp <= timestamp) {
                final Cdcpb.Event.Row commitRow = commits.pollFirstEntry().getValue();
                final Cdcpb.Event.Row prewriteRow =
                        prewrites.remove(RowKeyWithTs.ofStart(commitRow));
                // if pull cdc event block when region split, cdc event will lose.
                committedEvents.offer(prewriteRow);
            }
        }
    }

    @Override
    public void cancel() {
        try {
            running = false;
            if (cdcClient != null) {
                cdcClient.close();
            }
            if (executorService != null) {
                executorService.shutdown();
                if (executorService.awaitTermination(CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the tidb source function in {} seconds.",
                            CLOSE_TIMEOUT);
                }
            }
        } catch (final Exception e) {
            LOG.error("Unable to close cdcClient", e);
        }
    }

    @Override
    public void snapshotState(final FunctionSnapshotContext context) throws Exception {
        LOG.info(
                "snapshotState checkpoint: {} at {} {} resolvedTs: {}",
                context.getCheckpointId(),
                database,
                tableName,
                resolvedTs);
        flushRows(resolvedTs);
        offsetState.clear();
        offsetState.add(resolvedTs);
    }

    @Override
    public void initializeState(final FunctionInitializationContext context) throws Exception {
        LOG.info("initialize checkpoint {} {} {}", database, tableName);

        offsetState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "resolvedTsState", LongSerializer.INSTANCE));

        if (context.isRestored()) {
            for (final Long offset : offsetState.get()) {
                resolvedTs = offset;
                isInitalized = true;
                LOG.info("Restore State from resolvedTs: {} {}  {}", database, tableName, resolvedTs);
                return;
            }
        } else {
            resolvedTs = 0;
            LOG.info("Initialize State from resolvedTs: {} {} {}", database, tableName, resolvedTs);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return snapshotEventDeserializationSchema.getProducedType();
    }

    // ---------------------------------------
    // static Utils classes
    // ---------------------------------------
    private static class RowKeyWithTs implements Comparable<RowKeyWithTs> {
        private final long timestamp;
        private final RowKey rowKey;

        private RowKeyWithTs(final long timestamp, final RowKey rowKey) {
            this.timestamp = timestamp;
            this.rowKey = rowKey;
        }

        private RowKeyWithTs(final long timestamp, final byte[] key) {
            this(timestamp, RowKey.decode(key));
        }

        @Override
        public int compareTo(final RowKeyWithTs that) {
            int res = Long.compare(this.timestamp, that.timestamp);
            if (res == 0) {
                res = Long.compare(this.rowKey.getTableId(), that.rowKey.getTableId());
            }
            if (res == 0) {
                res = Long.compare(this.rowKey.getHandle(), that.rowKey.getHandle());
            }
            return res;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.timestamp, this.rowKey.getTableId(), this.rowKey.getHandle());
        }

        @Override
        public boolean equals(final Object thatObj) {
            if (thatObj instanceof RowKeyWithTs) {
                final RowKeyWithTs that = (RowKeyWithTs) thatObj;
                return this.timestamp == that.timestamp && this.rowKey.equals(that.rowKey);
            }
            return false;
        }

        static RowKeyWithTs ofStart(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getStartTs(), row.getKey().toByteArray());
        }

        static RowKeyWithTs ofCommit(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getCommitTs(), row.getKey().toByteArray());
        }
    }

    private static class OutputCollector<T> implements Collector<T> {

        private SourceContext<T> context;

        @Override
        public void collect(T record) {
            context.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
