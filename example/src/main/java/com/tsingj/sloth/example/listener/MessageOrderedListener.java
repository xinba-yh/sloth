package com.tsingj.sloth.example.listener;

import com.tsingj.sloth.client.consumer.ConsumeStatus;
import com.tsingj.sloth.client.consumer.MessageListener;
import com.tsingj.sloth.remoting.message.Remoting;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yanghao
 * one partition one thread.
 */
@Slf4j
public class MessageOrderedListener implements MessageListener {

    @Override
    public ConsumeStatus consumeMessage(Remoting.GetMessageResult.Message msg) {
        log.info("Thread:{} topic:{} partition:{} offset:{}", Thread.currentThread().getId(), msg.getTopic(), msg.getPartition(), msg.getOffset());
        return ConsumeStatus.SUCCESS;
    }

}
