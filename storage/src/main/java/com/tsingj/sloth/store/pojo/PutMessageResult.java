package com.tsingj.sloth.store.pojo;


/**
 * @author yanghao
 */
public class PutMessageResult {

    public PutMessageResult(PutMessageStatus status, String topic, int partition, long offset) {
        this.status = status;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public PutMessageResult(PutMessageStatus status, String errorMsg) {
        this.status = status;
        this.errorMsg = errorMsg;
    }

    public PutMessageResult(PutMessageStatus status) {
        this.status = status;
    }


    /**
     * 状态
     */
    private PutMessageStatus status;

    /**
     * 错误信息
     */
    private String errorMsg;


    /**
     * topic
     */
    private String topic;

    /**
     * partition
     */
    private Integer partition;

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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }
}
