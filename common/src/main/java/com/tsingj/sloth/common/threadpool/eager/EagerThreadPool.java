package com.tsingj.sloth.common.threadpool.eager;



import com.tsingj.sloth.common.threadpool.TaskThreadFactory;
import com.tsingj.sloth.common.threadpool.ThreadPool;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author yanghao
 * <p>
 * 1、当线程池中线程数量小于 corePoolSize 则创建线程，并处理请求。
 * 2、当线程池中线程数量大于等于 corePoolSize 时，则创建新线程而不是将任务放入阻塞队列。
 * 3、当创建线程数达到 maximumPoolSize（最大线程数量设置值）时，把请求放入workQueue中,等待处理。
 * 4、如果队列长度大与设置值 queueSize时 使用 RejectedExecutionHandler 来进行任务拒绝处理。
 * 其他: 当非corePool指定keepAliveTimeSeconds不活跃时,线程池将其回收。
 * <p>
 */
public class EagerThreadPool implements ThreadPool {

    @Override
    public Executor getExecutor(int corePoolSize, int maximumPoolSize, int queueCapacity, long keepAliveTimeSeconds, String threadNamePrefix) {
        // init queue and executor
        TaskQueue<Runnable> taskQueue = new TaskQueue<>(queueCapacity <= 0 ? 1 : queueCapacity);
        EagerThreadPoolExecutor executor = new EagerThreadPoolExecutor(corePoolSize,
                maximumPoolSize,
                keepAliveTimeSeconds,
                TimeUnit.SECONDS,
                taskQueue,
                new TaskThreadFactory(threadNamePrefix),
                new ThreadPoolExecutor.AbortPolicy());
        taskQueue.setExecutor(executor);
        return executor;
    }

}
