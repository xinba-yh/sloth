package com.tsingj.sloth.broker.handler.processor;

import com.tsingj.sloth.broker.service.ConsumerGroupManager;
import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.remoting.RemoteRequestProcessor;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.List;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class CgHeartBeatProcessor implements RemoteRequestProcessor {

    private final ConsumerGroupManager consumerGroupManager;

    public CgHeartBeatProcessor(ConsumerGroupManager consumerGroupManager) {
        this.consumerGroupManager = consumerGroupManager;
    }

    @Override
    public byte getCommand() {
        return ProtocolConstants.Command.CONSUMER_GROUP_HEARTBEAT;
    }

    @Override
    public DataPackage process(DataPackage request, ChannelHandlerContext ctx) throws Exception {
        log.debug("receive CONSUMER_GROUP_HEARTBEAT command，correlationId:{}",request.getCorrelationId());
        Remoting.ConsumerHeartbeatRequest consumerHeartbeatRequest = Remoting.ConsumerHeartbeatRequest.parseFrom(request.getData());
        /*
         * check and set default param
         */
        String clientId = consumerHeartbeatRequest.getClientId();
        if (ObjectUtils.isEmpty(clientId)) {
            return this.respError(request, "IllegalArgument clientId is empty!");
        }
        String topic = consumerHeartbeatRequest.getTopic();
        if (ObjectUtils.isEmpty(topic)) {
            return this.respError(request, "IllegalArgument topic is empty!");
        }
        String groupName = consumerHeartbeatRequest.getGroupName();
        if (ObjectUtils.isEmpty(groupName)) {
            return this.respError(request, "IllegalArgument groupName is empty!");
        }

        Result<List<Integer>> heartbeatResult = consumerGroupManager.heartbeat(clientId, groupName, topic, ctx.channel());
        DataPackage dataPackage = heartbeatResult.success() ? this.respSuccess(request, heartbeatResult.getData()) : this.respError(request, heartbeatResult.getMsg());
        log.info("CONSUMER_GROUP_HEARTBEAT command done，correlationId:{}",request.getCorrelationId());
        return dataPackage;

    }

    private DataPackage respError(DataPackage request, String errMsg) {
        log.warn("process command CONSUMER_HEARTBEAT fail! {}", errMsg);
        Remoting.ConsumerHeartbeatResult sendResult = Remoting.ConsumerHeartbeatResult.newBuilder()
                .setRetCode(Remoting.RetCode.ERROR)
                .setErrorInfo(errMsg)
                .build();
        DataPackage response = request;
        response.setTimestamp(SystemClock.now());
        response.setData(sendResult.toByteArray());
        return response;
    }

    private DataPackage respSuccess(DataPackage request, List<Integer> consumerPartitions) {
        Remoting.ConsumerHeartbeatResult sendResult = Remoting.ConsumerHeartbeatResult.newBuilder()
                .setRetCode(Remoting.RetCode.SUCCESS)
                .addAllPartitions(consumerPartitions)
                .build();
        DataPackage response = request;
        response.setTimestamp(SystemClock.now());
        response.setData(sendResult.toByteArray());
        return response;
    }

}
