package com.tsingj.sloth.common.threadpool;

import java.util.concurrent.Executor;

/**
 * @author yanghao
 */
public interface ThreadPool {

    public Executor getExecutor(int corePoolSize, int maximumPoolSize, int queueCapacity, long keepAliveTimeSeconds,String threadNamePrefix);

}
