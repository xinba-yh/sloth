package com.tsingj.sloth.store;

/**
 * @author yanghao
 */

public enum GetMessageStatus {

    TOPIC_ILLEGAL,

    FOUND,

    OFFSET_FOUND_NULL,

    MESSAGE_DECODE_FAIL,

    PARTITION_NO_MESSAGE,

}
