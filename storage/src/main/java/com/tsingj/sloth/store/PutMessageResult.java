package com.tsingj.sloth.store;


/**
 * @author yanghao
 */
public class PutMessageResult {

    /**
     * 状态
     */
    private PutMessageStatus status;

    /**
     * 错误信息
     */
    private String errorMsg;

    /**
     * offset
     */
    private Long offset;

    public PutMessageStatus getStatus() {
        return status;
    }

    public void setStatus(PutMessageStatus status) {
        this.status = status;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public PutMessageResult(PutMessageStatus status) {
        this.status = status;
    }

    public PutMessageResult(PutMessageStatus status, String errorMsg) {
        this.status = status;
        this.errorMsg = errorMsg;
    }

    public PutMessageResult(PutMessageStatus status, Long offset) {
        this.status = status;
        this.offset = offset;
    }
}
