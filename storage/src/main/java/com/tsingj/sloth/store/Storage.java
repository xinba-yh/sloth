package com.tsingj.sloth.store;

import com.tsingj.sloth.store.pojo.GetMessageResult;
import com.tsingj.sloth.store.pojo.Message;
import com.tsingj.sloth.store.pojo.PutMessageResult;

/**
 * @author yanghao
 */
public interface Storage {

    /**
     * 写入消息
     */
    PutMessageResult putMessage(Message message);


    /**
     * 获取指定offset消息
     */
    GetMessageResult getMessage(String topic, int partitionId, long offset);


    /**
     * 获取topic-partition最大offset
     * @param topic
     * @param partitionId
     * @return
     */
    long getMaxOffset(String topic, int partitionId);

    /**
     * 获取topic-partition最小offset
     * @param topic
     * @param partitionId
     * @return
     */
    long getMinOffset(String topic, int partitionId);


}
