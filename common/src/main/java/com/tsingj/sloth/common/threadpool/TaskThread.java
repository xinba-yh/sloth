package com.tsingj.sloth.common.threadpool;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yanghao on 2017/8/29.
 * 作用：自定义线程名
 */
public class TaskThread extends Thread{

    private static final AtomicInteger created = new AtomicInteger(0);

    public TaskThread(Runnable r, String name){//每次创建一批，这里会清空
        super(r,name+"-"+generateSeq());
    }

    private static int generateSeq() {
        int seq = created.incrementAndGet();
        if(seq >= 200){
            created.set(0);
        }
        return seq;
    }

    @Override
    public void run() {
        super.run();
    }

}
