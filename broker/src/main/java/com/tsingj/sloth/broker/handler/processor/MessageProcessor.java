package com.tsingj.sloth.broker.handler.processor;

import com.google.protobuf.ByteString;
import com.tsingj.sloth.remoting.RemoteRequestProcessor;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import com.tsingj.sloth.store.topic.TopicConfig;
import com.tsingj.sloth.store.topic.TopicManager;
import com.tsingj.sloth.store.StorageEngine;
import com.tsingj.sloth.store.pojo.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.Map;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class MessageProcessor implements RemoteRequestProcessor {

    private final TopicManager topicManager;

    private final StorageEngine storageEngine;

    public MessageProcessor(StorageEngine storageEngine, TopicManager topicManager) {
        this.storageEngine = storageEngine;
        this.topicManager = topicManager;
    }

    @Override
    public byte getCommand() {
        return ProtocolConstants.Command.SEND_MESSAGE;
    }

    @Override
    public DataPackage process(DataPackage request) throws Exception {
        Remoting.Message msg = Remoting.Message.parseFrom(request.getData());

        /*
         * check and set default param
         */
        String topic = msg.getTopic();
        if (ObjectUtils.isEmpty(topic)) {
            return this.respError(request, "IllegalArgument topic is empty!");
        }
        ByteString body = msg.getBody();
        if (body.isEmpty()) {
            return this.respError(request, "IllegalArgument messageBody is empty!");
        }
        //get topicConfig, not exist create
        Result<TopicConfig> topicResult = topicManager.getTopic(topic, true);
        if (topicResult.failure()) {
            return this.respError(request, topicResult.getMsg());
        }
        TopicConfig topicConfig = topicResult.getData();
        int partition = msg.getPartition();
        if (partition > topicConfig.getPartition()) {
            return this.respError(request, "IllegalArgument partition is too large! topic:" + topic + " maximum:" + topicConfig.getPartition());
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
            return this.respSuccess(request, putMessageResult);
        } else {
            return this.respError(request, putMessageResult.getStatus() + ":" + putMessageResult.getErrorMsg());
        }

    }

    private DataPackage respError(DataPackage request, String errMsg) {
        log.warn("process command sendMessage fail! {}", errMsg);
        Remoting.SendResult sendResult = Remoting.SendResult.newBuilder()
                .setRetCode(Remoting.SendResult.RetCode.ERROR)
                .setErrorInfo(errMsg)
                .build();
        DataPackage response = request;
        response.setTimestamp(System.currentTimeMillis());
        response.setData(sendResult.toByteArray());
        return response;
    }

    private DataPackage respSuccess(DataPackage request, PutMessageResult putMessageResult) {
        log.warn("process command sendMessage success! {} {} {}", putMessageResult.getTopic(), putMessageResult.getPartition(), putMessageResult.getOffset());
        Remoting.SendResult sendResult = Remoting.SendResult.newBuilder()
                .setRetCode(Remoting.SendResult.RetCode.SUCCESS)
                .setResultInfo(Remoting.SendResult.ResultInfo.newBuilder().setOffset(putMessageResult.getOffset()).setTopic(putMessageResult.getTopic()).setPartition(putMessageResult.getPartition()).build())
                .build();
        DataPackage response = request;
        response.setTimestamp(System.currentTimeMillis());
        response.setData(sendResult.toByteArray());
        return response;
    }

}
