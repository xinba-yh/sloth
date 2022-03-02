package com.tsingj.sloth.store.log.lock;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yanghao
 */
public class LogSpinLock implements LogLock {

    private final AtomicBoolean spinLock = new AtomicBoolean(true);

    @Override
    public void lock() {
        boolean flag;
        do {
            flag = this.spinLock.compareAndSet(true, false);
        }
        while (!flag);
    }

    @Override
    public void unlock() {
        this.spinLock.compareAndSet(false, true);
    }

}
