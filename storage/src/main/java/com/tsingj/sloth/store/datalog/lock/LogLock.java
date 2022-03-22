package com.tsingj.sloth.store.datalog.lock;

/**
 * @author yanghao
 */
public interface LogLock {

    void lock();

    void unlock();

}
