package com.tsingj.sloth.client;

import com.tsingj.sloth.client.consumer.SlothConsumerManager;
import com.tsingj.sloth.client.consumer.SlothRemoteConsumer;
import com.tsingj.sloth.client.consumer.TopicPartitionConsumer;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.RemoteCommand;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


/**
 * @author yanghao
 */
@Slf4j
public class RemoteClientHandler extends SimpleChannelInboundHandler<RemoteCommand> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemoteCommand remoteCommand) throws Exception {
        //同步消息
        if (ProtocolConstants.RequestType.SYNC == remoteCommand.getRequestType()) {
            //client主动发送消息，等待响应
            Long correlationId = remoteCommand.getCorrelationId();
            log.debug("client receive CONSUMER_GROUP_HEARTBEAT correlationId:{}", correlationId);
            byte[] responseData = remoteCommand.getData();
            ResponseFuture responseFuture = RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.get(correlationId);
            if (responseFuture == null) {
                log.warn("invalid correlationId:{}!", correlationId);
                return;
            }
            if (responseData == null) {
                log.warn("correlationId:{} response null!", correlationId);
                return;
            }
            responseFuture.putResponse(remoteCommand);
            //broker主动消息，client需要回复
            // TODO: 2022/4/1 暂无此类消息。
        } else {
            if (remoteCommand.getCommand() == ProtocolConstants.Command.BROKER_NOTIFY) {
                log.info("receive BROKER_NOTIFY.");
                Remoting.Notify notify = Remoting.Notify.parseFrom(remoteCommand.getData());
                //process RE_BALANCE_BROADCAST event
                if (notify.getEvent() == Remoting.Notify.Event.RE_BALANCE_BROADCAST) {
                    Remoting.Notify.TopicConsumer topicConsumer = notify.getTopicConsumer();
                    SlothRemoteConsumer slothRemoteConsumer = SlothConsumerManager.get(topicConsumer.getTopic());
                    if (slothRemoteConsumer == null) {
                        log.warn("find topic consumer fail!");
                        return;
                    }
                    List<Integer> shouldConsumerPartitions = topicConsumer.getPartitionsList();
                    slothRemoteConsumer.reBalanceCheck(shouldConsumerPartitions);
                }
                //process CONSUMER_WEEK_UP event
                else if (notify.getEvent() == Remoting.Notify.Event.CONSUMER_WEEK_UP) {
                    Remoting.Notify.TopicConsumerPartition topicConsumerPartition = notify.getTopicConsumerPartition();
                    SlothRemoteConsumer slothRemoteConsumer = SlothConsumerManager.get(topicConsumerPartition.getTopic());
                    if (slothRemoteConsumer == null) {
                        log.warn("find topic consumer fail!");
                        return;
                    }
                    TopicPartitionConsumer topicPartitionConsumer = slothRemoteConsumer.getTopicPartitionConsumer(topicConsumerPartition.getPartition());
                    if (topicPartitionConsumer == null) {
                        log.warn("current topic consumer not consume partition:{}!", topicConsumerPartition.getPartition());
                        return;
                    }
                    topicPartitionConsumer.weekUp();
                }
            } else {
                log.warn("unsupported command:{}!", remoteCommand.getCommand());
            }
        }
    }

}
