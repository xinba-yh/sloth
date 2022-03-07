package com.tsingj.sloth.broker.grpc.handler;

import com.google.protobuf.ByteString;
import com.tsingj.sloth.rpcmodel.grpc.protobuf.NotificationOuterClass;
import com.tsingj.sloth.store.topic.TopicConfig;
import com.tsingj.sloth.store.topic.TopicManager;
import com.tsingj.sloth.store.StorageEngine;
import com.tsingj.sloth.store.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.Map;

/**
 * @author yanghao
 */
@Component
public class MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private final TopicManager topicManager;

    private final StorageEngine storageEngine;

    public MessageHandler(StorageEngine storageEngine, TopicManager topicManager) {
        this.storageEngine = storageEngine;
        this.topicManager = topicManager;
    }

    public NotificationOuterClass.SendResult storeMessage(NotificationOuterClass.SendRequest.Message msg) {
        String requestId = msg.getRequestId();
        /*
         * check and set default param
         */
        String topic = msg.getTopic();
        if (ObjectUtils.isEmpty(topic)) {
            return this.respError(requestId, "IllegalArgument topic is empty!");
        }
        ByteString body = msg.getBody();
        if (body.isEmpty()) {
            return this.respError(requestId, "IllegalArgument messageBody is empty!");
        }
        //get topicConfig, not exist create
        Result<TopicConfig> topicResult = topicManager.getTopic(topic, true);
        if (topicResult.failure()) {
            return this.respError(requestId, topicResult.getMsg());
        }
        TopicConfig topicConfig = topicResult.getData();
        int partition = msg.getPartition();
        if (partition > topicConfig.getPartition()) {
            return this.respError(requestId, "IllegalArgument partition is too large! topic:" + topic + " maximum:" + topicConfig.getPartition());
        }
        //use assign partition or auto assign , partition start 1 -> partition size
        if (partition == 0) {
            partition = topicManager.autoAssignPartition(topicConfig);
        }
        Map<String, String> properties = msg.getPropertiesMap();
        /*
         * convert message
         */
        Message message = new Message();
        message.setTopic(topic);
        message.setPartition(partition);
        message.setBody(body.toByteArray());
        message.setProperties(properties);
        /*
         * store message
         */
        PutMessageResult putMessageResult = storageEngine.putMessage(message);
        if (putMessageResult.getStatus() == PutMessageStatus.OK) {
            return NotificationOuterClass.SendResult.newBuilder()
                    .setResponseType(NotificationOuterClass.SendResult.SendResponseType.ACK)
                    .setAck(NotificationOuterClass.SendResult.Ack.newBuilder()
                            .setRetCode(NotificationOuterClass.SendResult.Ack.RetCode.SUCCESS)
                            .setRequestId(requestId)
                            .setResultInfo(NotificationOuterClass.SendResult.Ack.ResultInfo.newBuilder()
                                    .setTopic(topic)
                                    .setPartition(partition)
                                    .setOffset(putMessageResult.getOffset())
                                    .build())
                            .build()).build();
        } else {
            return this.respError(requestId, putMessageResult.getStatus() + ":" + putMessageResult.getErrorMsg());
        }
    }

    private NotificationOuterClass.SendResult respError(String requestId, String errMsg) {
        return NotificationOuterClass.SendResult.newBuilder()
                .setResponseType(NotificationOuterClass.SendResult.SendResponseType.ACK)
                .setAck(NotificationOuterClass.SendResult.Ack.newBuilder()
                        .setRetCode(NotificationOuterClass.SendResult.Ack.RetCode.ERROR)
                        .setInfo(errMsg)
                        .setRequestId(requestId)
                        .build()).build();
    }

}
