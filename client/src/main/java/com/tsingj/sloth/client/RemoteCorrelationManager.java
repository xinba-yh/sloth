package com.tsingj.sloth.client;

import com.tsingj.sloth.remoting.ResponseFuture;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yanghao
 */
public class RemoteCorrelationManager {

    /**
     * The correlation id.
     * 并发问题，可使用jdk8 LongAdder替代。
     */
    public static final AtomicLong CORRELATION_ID = new AtomicLong(1);

    /**
     * correlation id -> responseMap
     */
    public static final ConcurrentMap<Long /* correlationId */, ResponseFuture> CORRELATION_ID_RESPONSE_MAP = new ConcurrentHashMap<>(256);


    //todo add clear


}
