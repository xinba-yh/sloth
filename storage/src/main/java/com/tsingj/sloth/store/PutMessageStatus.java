package com.tsingj.sloth.store;

/**
 * @author yanghao
 */

public enum PutMessageStatus {
    //成功
    OK,
    //日志文件操作失败
    LOG_FILE_OPERATION_FAIL,
    //encode失败
    DATA_ENCODE_FAIL,
    //消息验证失败
    MESSAGE_ILLEGAL,
    //未知异常
    UNKNOWN_ERROR,
}
