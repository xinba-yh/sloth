package com.tsingj.sloth.store.mock;

import com.tsingj.sloth.store.StorageEngine;
import com.tsingj.sloth.store.pojo.Message;
import com.tsingj.sloth.store.pojo.PutMessageResult;
import com.tsingj.sloth.store.pojo.PutMessageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class SendMessage implements Runnable{

    private Logger LOG = LoggerFactory.getLogger(SendMessage.class);

    private StorageEngine storageEngine;

    private Message message;

    public SendMessage(StorageEngine storageEngine, Message message) {
        this.storageEngine = storageEngine;
        this.message = message;
    }

    @Override
    public void run() {
        System.out.println("----------------------------");
        PutMessageResult putMessageResult = storageEngine.putMessage(message);
        if (putMessageResult.getStatus() != PutMessageStatus.OK) {
            LOG.warn("putMessage fail! {}", putMessageResult.getErrorMsg());
        } else {
            LOG.info("partition:{} respOffset:{}", message.getPartition(), putMessageResult.getOffset());
        }
        System.out.println("!----------------------------!");
    }

}
