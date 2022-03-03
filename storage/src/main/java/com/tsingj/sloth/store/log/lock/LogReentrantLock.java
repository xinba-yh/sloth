package com.tsingj.sloth.store.log.lock;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yanghao
 */
public class LogReentrantLock implements LogLock {

    private ReentrantLock lock = new ReentrantLock();

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

}
