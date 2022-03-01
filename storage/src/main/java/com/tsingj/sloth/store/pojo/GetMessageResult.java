package com.tsingj.sloth.store.pojo;


/**
 * @author yanghao
 */
public class GetMessageResult {

    /**
     * 状态
     */
    private GetMessageStatus status;

    /**
     * 错误信息
     */
    private String errorMsg;


    private Message message;

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public GetMessageResult() {

    }

    public GetMessageResult(GetMessageStatus status) {
        this.status = status;
    }

    public GetMessageResult(GetMessageStatus status, String errorMsg) {
        this.status = status;
        this.errorMsg = errorMsg;
    }

    public GetMessageResult(GetMessageStatus status, Message message) {
        this.status = status;
        this.message = message;
    }

    @Override
    public String toString() {
        return "GetMessageResult{" +
                "status=" + status +
                ", errorMsg='" + errorMsg + '\'' +
                ", message=" + message +
                '}';
    }
}
