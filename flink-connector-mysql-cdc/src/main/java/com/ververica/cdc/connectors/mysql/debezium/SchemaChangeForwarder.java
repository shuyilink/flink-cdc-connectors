package com.ververica.cdc.connectors.mysql.debezium;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SchemaChangeForwarder implements DatabaseHistory {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeForwarder.class);

    public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

    public static final ConcurrentMap<String, Collection<TableChanges.TableChange>> TABLE_SCHEMAS =
            new ConcurrentHashMap<>();

    private Map<TableId, TableChanges.TableChange> tableSchemas;
    private DatabaseHistoryListener listener;
    private boolean storeOnlyMonitoredTablesDdl;
    private boolean skipUnparseableDDL;

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            DatabaseHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        this.listener = listener;
        this.storeOnlyMonitoredTablesDdl = config.getBoolean(STORE_ONLY_MONITORED_TABLES_DDL);
        this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);

        // recover
        String instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
        this.tableSchemas = new HashMap<>();
        for (TableChanges.TableChange tableChange : removeHistory(instanceName)) {
            tableSchemas.put(tableChange.getId(), tableChange);
        }
    }

    @Override
    public void start() {
        listener.started();
    }

    @Override
    public void record(
            Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl)
            throws DatabaseHistoryException {
        throw new UnsupportedOperationException("should not call here, error");
    }

    @Override
    public void record(
            Map<String, ?> source,
            Map<String, ?> position,
            String databaseName,
            String schemaName,
            String ddl,
            TableChanges changes) {
        LOG.info(
                "SchemaChangeForwarder record {} {} {} {} {} {}",
                databaseName,
                schemaName,
                ddl,
                changes,
                source,
                position);
        try {
            DDlSyncExecutor.getInstance().execute(databaseName,ddl);
        }catch (Exception e) {
            LOG.info("SchemaChangeForwarder execute failed {}",e.toString());
        }
    }

    @Override
    public void recover(
            Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser ddlParser) {
        listener.recoveryStarted();
        for (TableChanges.TableChange tableChange : tableSchemas.values()) {
            schema.overwriteTable(tableChange.getTable());
        }
        listener.recoveryStopped();
    }

    @Override
    public void stop() {
        listener.stopped();
    }

    @Override
    public boolean exists() {
        return tableSchemas != null && !tableSchemas.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public void initializeStorage() {
        // do nothing
    }

    @Override
    public boolean storeOnlyCapturedTables() {
        return storeOnlyMonitoredTablesDdl;
    }

    @Override
    public boolean skipUnparseableDdlStatements() {
        return skipUnparseableDDL;
    }

    public static void registerHistory(String engineName, Collection<TableChanges.TableChange> engineHistory) {
        TABLE_SCHEMAS.put(engineName, engineHistory);
    }

    public static Collection<TableChanges.TableChange> removeHistory(String engineName) {
        if (engineName == null) {
            return Collections.emptyList();
        }
        Collection<TableChanges.TableChange> tableChanges = TABLE_SCHEMAS.remove(engineName);
        return tableChanges != null ? tableChanges : Collections.emptyList();
    }
}
