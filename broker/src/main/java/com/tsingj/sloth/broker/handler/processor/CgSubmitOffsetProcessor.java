package com.tsingj.sloth.broker.handler.processor;

import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.remoting.RemoteRequestProcessor;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.RemoteCommand;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import com.tsingj.sloth.store.datajson.offset.ConsumerGroupOffsetManager;
import com.tsingj.sloth.store.datajson.topic.TopicConfig;
import com.tsingj.sloth.store.datajson.topic.TopicManager;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class CgSubmitOffsetProcessor implements RemoteRequestProcessor {

    private final ConsumerGroupOffsetManager consumerGroupOffsetManager;

    private final TopicManager topicManager;

    public CgSubmitOffsetProcessor(TopicManager topicManager, ConsumerGroupOffsetManager consumerGroupOffsetManager) {
        this.topicManager = topicManager;
        this.consumerGroupOffsetManager = consumerGroupOffsetManager;
    }

    @Override
    public byte getCommand() {
        return ProtocolConstants.Command.SUBMIT_CONSUMER_GROUP_OFFSET;
    }

    @Override
    public RemoteCommand process(RemoteCommand request, ChannelHandlerContext ctx) throws Exception {
        log.debug("receive SUBMIT_CONSUMER_GROUP_OFFSET command.");
        Remoting.SubmitConsumerOffsetRequest submitConsumerOffsetRequest = Remoting.SubmitConsumerOffsetRequest.parseFrom(request.getData());

        /*
         * check and set default param
         */
        String topic = submitConsumerOffsetRequest.getTopic();
        if (ObjectUtils.isEmpty(topic)) {
            return this.respError(request, "IllegalArgument topic is empty!");
        }
        String groupName = submitConsumerOffsetRequest.getGroupName();
        if (ObjectUtils.isEmpty(groupName)) {
            return this.respError(request, "IllegalArgument groupName is empty!");
        }

        Result<TopicConfig> topicConfigResult = topicManager.getTopic(topic, false);
        if (topicConfigResult.failure() || topicConfigResult.getData() == null) {
            return this.respError(request, "topic " + topic + " invalid !");
        }
        int partition = submitConsumerOffsetRequest.getPartition();
        long offset = submitConsumerOffsetRequest.getOffset();

        consumerGroupOffsetManager.commitOffset(groupName, topic, partition, offset);
        return this.respSuccess(request);

    }

    private RemoteCommand respError(RemoteCommand request, String errMsg) {
        log.warn("process command SUBMIT_CONSUMER_OFFSET fail! {}", errMsg);
        Remoting.SubmitConsumerOffsetResult result = Remoting.SubmitConsumerOffsetResult.newBuilder()
                .setRetCode(Remoting.RetCode.ERROR)
                .setErrorInfo(errMsg)
                .build();
        RemoteCommand response = request;
        response.setTimestamp(SystemClock.now());
        response.setData(result.toByteArray());
        return response;
    }

    private RemoteCommand respSuccess(RemoteCommand request) {
        Remoting.SubmitConsumerOffsetResult result = Remoting.SubmitConsumerOffsetResult.newBuilder()
                .setRetCode(Remoting.RetCode.SUCCESS)
                .build();
        RemoteCommand response = request;
        response.setTimestamp(SystemClock.now());
        response.setData(result.toByteArray());
        return response;
    }

}
