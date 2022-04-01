package com.tsingj.sloth.common.threadpool.fixed;



import com.tsingj.sloth.common.threadpool.TaskThreadFactory;
import com.tsingj.sloth.common.threadpool.ThreadPool;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author yanghao
 * <p>
 * 1、当线程池中线程数量小于 corePoolSize 则创建线程，并处理请求。
 * 2、当线程池中线程数量大于等于 corePoolSize 时，则把请求放入 workQueue 中,随着线程池中的核心线程们不断执行任务，只要线程池中有空闲的核心线程，线程池就从workQueue 中取任务并处理。
 * 3、当 workQueue 已存满，放不下新任务时则新建非核心线程入池，并处理请求直到线程数目达到 maximumPoolSize（最大线程数量设置值）。
 * 4、如果线程池中线程数大于 maximumPoolSize 则使用 RejectedExecutionHandler 来进行任务拒绝处理。
 * 其他: 当非corePool指定keepAliveTimeSeconds不活跃时,线程池将其回收。
 * <p>
 */
public class FixedThreadPool implements ThreadPool {

    @Override
    public Executor getExecutor(int corePoolSize, int maximumPoolSize, int queueCapacity, long keepAliveTimeSeconds, String threadNamePrefix) {
        //（调用者运行）策略实现了一种调节机制，该策略既不会抛弃任务，也不会抛出异常，而是将某些任务退回到调用者，从而降低新任务的流量。任务会被保存在tcp层，并且由于主线程短时间不会调用accept，持续过载也会出现消息丢失。
        return new FixedThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTimeSeconds, TimeUnit.SECONDS, new LinkedBlockingQueue<>(queueCapacity), new TaskThreadFactory(threadNamePrefix), new ThreadPoolExecutor.AbortPolicy());
    }

}
