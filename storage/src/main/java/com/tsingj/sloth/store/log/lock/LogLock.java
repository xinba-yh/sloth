package com.tsingj.sloth.store.log.lock;

/**
 * @author yanghao
 */
public interface LogLock {

    void lock();

    void unlock();

}
