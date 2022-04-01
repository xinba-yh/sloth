package com.tsingj.sloth.common.threadpool.fixed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 *
 * @author yanghao
 * @date 2017/8/29
 */
public class FixedThreadPoolExecutor extends ThreadPoolExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FixedThreadPoolExecutor.class);

    private BlockingQueue<Runnable> workQueue;

    public FixedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        this.workQueue = workQueue;
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        try {
            if (workQueue.size() > 0) {
                LOGGER.info("FixedThreadPoolExecutor task queue size:" + workQueue.size());
            }
        } finally {
            super.afterExecute(r, t);
        }
    }

    @Override
    protected void terminated() {
        super.terminated();
    }

}
