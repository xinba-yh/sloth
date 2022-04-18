package com.tsingj.sloth.broker.service;

import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.common.result.Results;
import com.tsingj.sloth.remoting.ChannelAttributeConstants;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.RemoteCommand;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import io.netty.channel.Channel;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class ConsumerGroupManager {

    private final TopicManager topicManager;

    public ConsumerGroupManager(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    private static final String TOPIC_GROUP_SEPARATOR = "@";
    /**
     * topic@group -> partition - offset
     */
    private final static ConcurrentMap<String, ConcurrentHashMap<String, ConsumerChannel>> TOPIC_GROUP_CONSUMER_CHANNEL_MAP = new ConcurrentHashMap<>(8);


    public Result<List<Integer>> heartbeat(String clientId, String groupName, String topic, Channel channel) {
        //setup channel clientId attr
        channel.attr(ChannelAttributeConstants.CLIENT_ID).set(clientId);

        //get topicConfig, not exist create
        Result<TopicManager.TopicConfig> topicResult = topicManager.getTopic(topic, true);
        if (topicResult.failure()) {
            return Results.failure(topicResult.getMsg());
        }

        AtomicBoolean newConsumer = new AtomicBoolean(false);
        ConcurrentHashMap<String, ConsumerChannel> topicGroupConsumerChannelMap = TOPIC_GROUP_CONSUMER_CHANNEL_MAP.computeIfAbsent(this.key(groupName, topic), s -> new ConcurrentHashMap<>(8));
        ConsumerChannel consumerChannel = topicGroupConsumerChannelMap.computeIfAbsent(clientId, s -> {
            newConsumer.set(true);
            return new ConsumerChannel(clientId, channel);
        });

        //新连接client
        if (newConsumer.get()) {
            //reBalance topic partition
            TopicManager.TopicConfig topicConfig = topicResult.getData();
            //assign partitions
            this.roundRibbonAssignPartitions(groupName, topic, topicConfig.getAllPartitions());
            //notify consumers reBalance
            this.notifyConsumerReBalanceEvent(groupName, topic, clientId);
        } else {
            //刷新心跳时间
            consumerChannel.setHbTimestamp(SystemClock.now());
        }
        return Results.success(consumerChannel.getPartitions());
    }


    public ConcurrentHashMap<String, ConsumerChannel> getConsumerChannelMap(String groupName, String topic) {
        return TOPIC_GROUP_CONSUMER_CHANNEL_MAP.get(this.key(groupName, topic));
    }


    private synchronized void notifyConsumerReBalanceEvent(String groupName, String topic, String excludeClientId) {
        ConcurrentHashMap<String, ConsumerChannel> consumerChannelMap = TOPIC_GROUP_CONSUMER_CHANNEL_MAP.get(this.key(groupName, topic));
        for (Map.Entry<String, ConsumerChannel> entry : consumerChannelMap.entrySet()) {
            String clientId = entry.getKey();
            if (excludeClientId.equalsIgnoreCase(clientId)) {
                continue;
            }
            ConsumerChannel consumerChannel = entry.getValue();
            List<Integer> partitions = consumerChannel.getPartitions();
            log.info("notify consumer clientId:{} reBalance partitions:{}.", clientId, partitions);
            RemoteCommand remoteCommand = this.buildNotify(groupName, topic, partitions);
            consumerChannel.getChannel().writeAndFlush(remoteCommand);
        }
    }

    private RemoteCommand buildNotify(String groupName, String topic, List<Integer> partitions) {
        Remoting.Notify.TopicConsumer topicConsumer = Remoting.Notify.TopicConsumer.newBuilder()
                .setGroup(groupName)
                .setTopic(topic)
                .addAllPartitions(partitions)
                .build();

        Remoting.Notify notify = Remoting.Notify.newBuilder()
                .setEvent(Remoting.Notify.Event.RE_BALANCE_BROADCAST)
                .setTopicConsumer(topicConsumer)
                .build();

        return RemoteCommand.builder()
                .magicCode(ProtocolConstants.MAGIC_CODE)
                .version(ProtocolConstants.VERSION)
                .command(ProtocolConstants.Command.BROKER_NOTIFY)
                .requestType(ProtocolConstants.RequestType.ONE_WAY)
                .timestamp(SystemClock.now())
                .data(notify.toByteArray())
                .build();
    }

    private synchronized void roundRibbonAssignPartitions(String groupName, String topic, List<Integer> allPartitions) {
        ConcurrentHashMap<String, ConsumerChannel> consumerChannelMap = TOPIC_GROUP_CONSUMER_CHANNEL_MAP.get(this.key(groupName, topic));

        //sorted on connect timestamp
        List<ConsumerChannel> consumerChannels = consumerChannelMap.values().stream().sorted(Comparator.comparing(ConsumerChannel::getTimestamp)).collect(Collectors.toList());

        //assign
        Map<String, List<Integer>> clientIdPartitions = new HashMap<>(consumerChannels.size());
        for (int i = 0; i < allPartitions.size(); i++) {
            int partition = allPartitions.get(i);
            int index = i % consumerChannels.size();
            ConsumerChannel consumerChannel = consumerChannels.get(index);
            List<Integer> partitions = clientIdPartitions.computeIfAbsent(consumerChannel.getClientId(), s -> new ArrayList<>());
            partitions.add(partition);
        }

        for (ConsumerChannel consumerChannel : consumerChannels) {
            List<Integer> partitions = clientIdPartitions.get(consumerChannel.getClientId());
            if (partitions != null && partitions.size() > 0) {
                List<Integer> sortedPartitions = partitions.stream().sorted().collect(Collectors.toList());
                consumerChannel.setPartitions(sortedPartitions);
            }
        }
    }

    private String key(String groupName, String topic) {
        return groupName + TOPIC_GROUP_SEPARATOR + topic;
    }

    @Data
    public static class ConsumerChannel {
        private String clientId;
        private Channel channel;
        private long timestamp = SystemClock.now();
        private long hbTimestamp = SystemClock.now();
        private volatile List<Integer> partitions = new ArrayList<>();

        public ConsumerChannel(String clientId, Channel channel) {
            this.clientId = clientId;
            this.channel = channel;
        }
    }


    public void removeClosedClient(String clientId) {
        for (Map.Entry<String, ConcurrentHashMap<String, ConsumerChannel>> entry : TOPIC_GROUP_CONSUMER_CHANNEL_MAP.entrySet()) {
            ConcurrentHashMap<String, ConsumerChannel> consumerChannelMap = entry.getValue();
            if (consumerChannelMap == null || consumerChannelMap.size() == 0) {
                continue;
            }
            consumerChannelMap.remove(clientId);
            log.info("client closed , remove group@topic:{} mapping client:{}.", entry.getKey(), clientId);
        }
    }

}
