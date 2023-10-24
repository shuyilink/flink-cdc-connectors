package com.ververica.cdc.connectors.tidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelController {

    private final static Logger logger = LoggerFactory.getLogger(ParallelController.class);
    private static ReentrantLock localLock = new ReentrantLock();
    private static ArrayList<ReentrantLock> lockList = new ArrayList<>();

    private static boolean isInited = false;

    private static int parallel = 3;

    private static long lastInitTimestampMills;
    // db cache expired time
    private static long expiredMills = 5 * 60 * 1000;

    public static ReentrantLock getLock(){
        String parallel_env = System.getenv("MAX_PARALLEL_ENV");
        if(parallel_env != null && !parallel_env.isEmpty()) {
            parallel = Integer.parseInt(parallel_env);
        }

        Random random = new Random();
        int idx = Math.abs(random.nextInt()) % parallel;

        logger.info("---------ParallelController lock idx {} {}",parallel,idx);
        long currTimestampMills =  System.currentTimeMillis();
        if(isInited && currTimestampMills - lastInitTimestampMills < expiredMills){
            return lockList.get(idx);
        }

        localLock.lock();
        currTimestampMills =  System.currentTimeMillis();
        if(isInited && currTimestampMills - lastInitTimestampMills < expiredMills){
            localLock.unlock();
            return  lockList.get(idx);
        }
        lockList.clear();
        logger.info("---------ParallelController regenerate");
        for (int i = 0; i < parallel; i++) {
            ReentrantLock lock = new ReentrantLock();
            lockList.add(lock);
        }
        isInited = true;
        lastInitTimestampMills =  System.currentTimeMillis();
        localLock.unlock();
        return lockList.get(idx);
    }
}
