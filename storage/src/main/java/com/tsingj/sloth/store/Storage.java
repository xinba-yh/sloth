package com.tsingj.sloth.store;

/**
 * @author yanghao
 */
public interface Storage {

    /**
     * 加载之前存储的数据
     */
    boolean load();


    /**
     * 关闭
     */
    void close();

    /**
     * 写入消息
     */
    PutMessageResult putMessage(Message message);


    /**
     * 获取指定offset消息
     */
    GetMessageResult getMessage(String topic, int partitionId, long offset);

}
