package com.tsingj.sloth.broker.handler.processor;

import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.remoting.RemoteRequestProcessor;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
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
public class CgGetOffsetProcessor implements RemoteRequestProcessor {

    private final ConsumerGroupOffsetManager consumerGroupOffsetManager;

    private final TopicManager topicManager;

    public CgGetOffsetProcessor(TopicManager topicManager, ConsumerGroupOffsetManager consumerGroupOffsetManager) {
        this.topicManager = topicManager;
        this.consumerGroupOffsetManager = consumerGroupOffsetManager;
    }

    @Override
    public byte getCommand() {
        return ProtocolConstants.Command.GET_CONSUMER_GROUP_OFFSET;
    }

    @Override
    public DataPackage process(DataPackage request, ChannelHandlerContext ctx) throws Exception {
        log.debug("receive GET_CONSUMER_GROUP_OFFSET command.");
        Remoting.GetConsumerOffsetRequest getConsumerOffsetRequest = Remoting.GetConsumerOffsetRequest.parseFrom(request.getData());

        /*
         * check and set default param
         */
        String topic = getConsumerOffsetRequest.getTopic();
        if (ObjectUtils.isEmpty(topic)) {
            return this.respError(request, "IllegalArgument topic is empty!");
        }
        String groupName = getConsumerOffsetRequest.getGroupName();
        if (ObjectUtils.isEmpty(groupName)) {
            return this.respError(request, "IllegalArgument groupName is empty!");
        }
        int partition = getConsumerOffsetRequest.getPartition();

        //get topicConfig, not exist create
        Result<TopicConfig> topicResult = topicManager.getTopic(topic, true);
        if (topicResult.failure()) {
            return this.respError(request, topicResult.getMsg());
        }
        /*
         * store message
         */
        long offset = consumerGroupOffsetManager.queryOffset(groupName, topic, partition);
        return this.respSuccess(request, offset);
    }

    private DataPackage respError(DataPackage request, String errMsg) {
        log.warn("process command GET_CONSUMER_OFFSET fail! {}", errMsg);
        Remoting.GetConsumerOffsetResult result = Remoting.GetConsumerOffsetResult.newBuilder()
                .setRetCode(Remoting.RetCode.ERROR)
                .setErrorInfo(errMsg)
                .build();
        DataPackage response = request;
        response.setTimestamp(System.currentTimeMillis());
        response.setData(result.toByteArray());
        return response;
    }

    private DataPackage respSuccess(DataPackage request, long offset) {
        Remoting.GetConsumerOffsetResult result = Remoting.GetConsumerOffsetResult.newBuilder()
                .setRetCode(Remoting.RetCode.SUCCESS)
                .setOffset(offset)
                .build();
        DataPackage response = request;
        response.setTimestamp(System.currentTimeMillis());
        response.setData(result.toByteArray());
        return response;
    }

}
