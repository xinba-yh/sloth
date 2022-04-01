package com.tsingj.sloth.common.threadpool;

import java.util.concurrent.ThreadFactory;

/**
 * Created by yanghao on 2017/8/29.
 * 作用：自定义线程名
 */
public class TaskThreadFactory implements ThreadFactory{

    private final String poolName;

    public TaskThreadFactory(String poolName){
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new TaskThread(r,poolName);
    }
}
