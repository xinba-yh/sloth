package com.tsingj.sloth.broker.service;

import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import com.tsingj.sloth.store.datajson.topic.TopicConfig;
import com.tsingj.sloth.store.datajson.topic.TopicManager;
import com.tsingj.sloth.store.pojo.Result;
import com.tsingj.sloth.store.pojo.Results;
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
        //get topicConfig, not exist create
        Result<TopicConfig> topicResult = topicManager.getTopic(topic, true);
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
            TopicConfig topicConfig = topicResult.getData();
            //assign partitions
            this.roundRibbonAssignPartitions(groupName, topic, topicConfig.getAllPartitions());
            //notify consumers reBalance
            this.notifyConsumerReBalanceEvent(groupName, topic, clientId);
        } else {
            //刷新心跳时间
            consumerChannel.setTimestamp(System.currentTimeMillis());
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
            log.info("clientId:{} consumer:{}.", clientId, partitions);
            DataPackage dataPackage = this.buildNotify(groupName,topic);
            consumerChannel.getChannel().writeAndFlush(dataPackage);
        }
    }

    private DataPackage buildNotify(String groupName, String topic) {
        Remoting.Notify.TopicConsumer topicConsumer = Remoting.Notify.TopicConsumer.newBuilder()
                .setGroup(groupName)
                .setTopic(topic)
                .build();

        Remoting.Notify notify = Remoting.Notify.newBuilder()
                .setEvent(Remoting.Notify.Event.RE_BALANCE_BROADCAST)
                .setTopicConsumer(topicConsumer)
                .build();

        return DataPackage.builder()
                .magicCode(ProtocolConstants.MAGIC_CODE)
                .version(ProtocolConstants.VERSION)
                .command(ProtocolConstants.Command.BROKER_NOTIFY)
                .requestType(ProtocolConstants.RequestType.ONE_WAY)
                .timestamp(System.currentTimeMillis())
                .data(notify.toByteArray())
                .build();
    }

    private synchronized void roundRibbonAssignPartitions(String groupName, String topic, List<Integer> allPartitions) {
        ConcurrentHashMap<String, ConsumerChannel> consumerChannelMap = TOPIC_GROUP_CONSUMER_CHANNEL_MAP.get(this.key(groupName, topic));

        //sorted on clientId
        List<ConsumerChannel> consumerChannels = consumerChannelMap.values().stream().sorted(Comparator.comparing(ConsumerChannel::getClientId)).collect(Collectors.toList());

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
                consumerChannel.setPartitions(partitions);
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
        private long timestamp = System.currentTimeMillis();
        private volatile List<Integer> partitions = new ArrayList<>();

        public ConsumerChannel(String clientId, Channel channel) {
            this.clientId = clientId;
            this.channel = channel;
        }
    }

}
