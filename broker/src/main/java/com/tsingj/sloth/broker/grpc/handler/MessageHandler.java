package com.tsingj.sloth.broker.grpc.handler;

import com.google.protobuf.ByteString;
import com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass;
import com.tsingj.sloth.store.topic.TopicConfig;
import com.tsingj.sloth.store.topic.TopicManager;
import com.tsingj.sloth.store.StorageEngine;
import com.tsingj.sloth.store.pojo.*;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.Map;

/**
 * @author yanghao
 */
@Component
public class MessageHandler {

    private final TopicManager topicManager;

    private final StorageEngine storageEngine;

    public MessageHandler(StorageEngine storageEngine, TopicManager topicManager) {
        this.storageEngine = storageEngine;
        this.topicManager = topicManager;
    }

    public Result storeMessage(NotificationOuterClass.SendRequest.Message msg) {
        /*
         * check and set default param
         */
        String topic = msg.getTopic();
        if (ObjectUtils.isEmpty(topic)) {
            return Results.failure("IllegalArgument topic is empty!");
        }
        String messageId = msg.getMessageId();
        if (ObjectUtils.isEmpty(messageId)) {
            return Results.failure("IllegalArgument messageId is empty!");
        }
        ByteString body = msg.getBody();
        if (body.isEmpty()) {
            return Results.failure("IllegalArgument messageBody is empty!");
        }
        //get topicConfig, not exist create
        Result<TopicConfig> topicResult = topicManager.getTopic(topic, true);
        if (topicResult.failure()) {
            return topicResult;
        }
        TopicConfig topicConfig = topicResult.getData();
        int partition = msg.getPartition();
        if (partition > topicConfig.getPartition()) {
            return Results.failure("IllegalArgument partition is too large! topic:" + topic + " maximum:" + topicConfig.getPartition());
        }
        //use assign partition or auto assign , partition start 1 -> partition size
        if (partition == 0) {
            partition = topicManager.autoAssignPartition(topicConfig);
        }
        Map<String, String> properties = msg.getPropertiesMap();
        /*
         * convert message
         */
        //todo 思考： 这层转换是否可以省略？
        Message message = new Message();
        message.setTopic(topic);
        message.setPartition(partition);
        message.setMessageId(messageId);
        message.setBody(body.toByteArray());
        message.setProperties(properties);
        /*
         * store message
         */
        PutMessageResult putMessageResult = storageEngine.putMessage(message);
        if (putMessageResult.getStatus() == PutMessageStatus.OK) {
            return Results.success(putMessageResult.getOffset());
        } else {
            return Results.failure(putMessageResult.getStatus() + ":" + putMessageResult.getErrorMsg());
        }
    }

}
