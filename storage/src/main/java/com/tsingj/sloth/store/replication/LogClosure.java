package com.tsingj.sloth.store.replication;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.tsingj.sloth.common.SystemClock;

/**
 * @author yanghao
 */
public class LogClosure implements Closure {

    private final LogOperation logOperation;

    private boolean success;

    private byte[] response;

    private String errMsg;

    private final Object lock;

    private final long startTimestamp;

    private final long timeoutMills;


    public LogClosure(LogOperation logOperation, long timeoutMills) {
        this.logOperation = logOperation;
        this.lock = new Object();
        this.timeoutMills = timeoutMills;
        this.startTimestamp = SystemClock.now();
    }

    @Override
    public void run(Status status) {
        if (status.isOk()) {
            this.setSuccess(true);
        } else {
            this.setSuccess(false);
            this.setErrMsg(status.getErrorMsg());
        }
        this.weekUp();
    }

    public LogOperation getLogOperation() {
        return logOperation;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public byte[] getResponse() {
        return response;
    }

    public void setResponse(byte[] response) {
        this.response = response;
    }

    public void waitTimeMills() {
        synchronized (this.lock) {
            try {
                this.lock.wait(this.timeoutMills);
            } catch (InterruptedException ignored) {
            }
        }
    }

    public void weekUp() {
        synchronized (this.lock) {
            this.lock.notify();
        }
    }

    public boolean timeouted() {
        return SystemClock.now() - startTimestamp >= timeoutMills;
    }

}
