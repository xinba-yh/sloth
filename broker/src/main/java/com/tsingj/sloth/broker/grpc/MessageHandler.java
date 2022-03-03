package com.tsingj.sloth.broker.grpc;

import com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass;
import com.tsingj.sloth.store.StorageEngine;
import com.tsingj.sloth.store.pojo.Message;
import com.tsingj.sloth.store.pojo.Result;
import com.tsingj.sloth.store.pojo.Results;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author yanghao
 */
@Component
public class MessageHandler {

    @Autowired
    private StorageEngine storageEngine;

    public Result storeMessage(NotificationOuterClass.SendRequest.Message msg) {
        //1、check message
        String topic = msg.getTopic();
        int partition = msg.getPartition();
        String messageId = msg.getMessageId();
//        Integer partition = msg.get
        //store message
        Message message = new Message();

        storageEngine.putMessage(message);
        return Results.success();
    }

}
