package com.tsingj.sloth.store.pojo;

/**
 * @author yanghao
 */

public enum PutMessageStatus {
    //成功
    OK,
    //日志文件追加失败
    LOG_FILE_APPEND_FAIL,
    //日志文件复制失败
    LOG_FILE_REPLICA_FAIL,
    //创建日志文件失败
    CREATE_LOG_FILE_FAILED,
    //encode失败
    DATA_ENCODE_FAIL,
    //消息验证失败
    MESSAGE_ILLEGAL,
    //未知异常
    UNKNOWN_ERROR,
    ;
}
