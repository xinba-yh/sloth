package com.tsingj.sloth.store.log;

/**
 * @author yanghao
 * 日志
 */
public class LogRecoveryException extends RuntimeException {


    public LogRecoveryException(String message, Throwable e) {
        super(message, e);
    }

    public LogRecoveryException(Throwable e) {
        super(e);
    }

}
