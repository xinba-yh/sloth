package com.tsingj.sloth.store.datalog;

/**
 * @author yanghao
 * 日志
 */
public class DataLogRecoveryException extends RuntimeException {


    public DataLogRecoveryException(String message, Throwable e) {
        super(message, e);
    }

    public DataLogRecoveryException(Throwable e) {
        super(e);
    }

}
