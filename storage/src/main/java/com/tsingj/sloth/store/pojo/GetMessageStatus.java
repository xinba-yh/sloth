package com.tsingj.sloth.store.pojo;

/**
 * @author yanghao
 */

public enum GetMessageStatus {

    TOPIC_ILLEGAL,

    FOUND,

    OFFSET_NOT_FOUND,

    LOG_SEGMENT_NOT_FOUND,

    MESSAGE_DECODE_FAIL,

    PARTITION_NO_MESSAGE,

    //未知异常
    UNKNOWN_ERROR,

}
