package com.tsingj.sloth.store.log.lock;

import com.tsingj.sloth.store.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yanghao
 * topic partition 粒度持有锁
 * 备注：每个topic partition同时只有一个文件进行插入。
 */
public class LogLockFactory {

    private static final Logger logger = LoggerFactory.getLogger(Log.class);

    private static final Map<String, LogSpinLock> SPIN_LOCK_MAP = new ConcurrentHashMap<>();

    private static final String LOG_KEY_SEPARATOR = "-";

    public static LogSpinLock getSpinLock(String topic, Integer partition) {
        String key = topic + LOG_KEY_SEPARATOR + partition;
        return SPIN_LOCK_MAP.computeIfAbsent(key, s -> {
            logger.info("create lock for key:{}", s);
            return new LogSpinLock();
        });
    }

}
