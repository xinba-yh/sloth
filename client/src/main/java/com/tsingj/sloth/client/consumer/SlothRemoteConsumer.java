package com.tsingj.sloth.client.consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.client.RemoteCorrelationManager;
import com.tsingj.sloth.client.SlothRemoteClient;
import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import com.tsingj.sloth.common.threadpool.TaskThreadFactory;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.stream.Collectors;


/**
 * @author yanghao
 */
@Slf4j
public class SlothRemoteConsumer {

    /**
     * The rpc client options.
     */
    private final SlothClientProperties clientProperties;

    private final ConsumerProperties consumerProperties;

    private final SlothRemoteClient slothRemoteClient;

    private MessageListener messageListener;

    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1, new TaskThreadFactory("consumer-hb-"));


    public SlothRemoteConsumer(SlothClientProperties clientProperties, ConsumerProperties consumerProperties, SlothRemoteClient slothRemoteClient) {
        this.clientProperties = clientProperties;
        this.consumerProperties = consumerProperties;
        this.slothRemoteClient = slothRemoteClient;
    }

    /**
     * partition - consumer线程对应关系
     */
    private final ConcurrentHashMap<Integer, TopicPartitionConsumer> topicPartitionConsumerMap = new ConcurrentHashMap<>();

    private String topic;

    private String groupName;

    public void registerListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void start() {
        this.topic = this.consumerProperties.getTopic();
        this.groupName = this.consumerProperties.getGroupName();
        //1.1、立即发送一次heartbeat，并与建立clientId与channel的绑定关系（client - server）。
        //1.2、响应分配好的partition
        List<Integer> topicPartitions = this.heartbeat();
        //2、TopicPartitionConsumerManager
        if (topicPartitions != null && topicPartitions.size() > 0) {
            //todo 定义max consumer
            for (Integer topicPartition : topicPartitions) {
                TopicPartitionConsumer topicPartitionConsumer = new TopicPartitionConsumer(this.groupName, this.topic, topicPartition);
                Thread thread = new Thread(topicPartitionConsumer);
                thread.setName(this.topic + "-" + topicPartition);
                thread.start();
                topicPartitionConsumerMap.put(topicPartition, topicPartitionConsumer);
            }
            log.info("sloth topic {} consumer {} partitions {} init done.", this.topic, slothRemoteClient.getClientId(), topicPartitions);
        }
        //3、启动心跳线程
        executorService.scheduleAtFixedRate(new HeartBeatTimerTask(this.topic), 3000, 3000, TimeUnit.MILLISECONDS);

    }

    public void destroy() {
        log.info("sloth consumer {} destroy.", this.topic);
        // TODO: 2022/3/31
        executorService.shutdown();
    }

    @Slf4j
    public static class HeartBeatTimerTask extends TimerTask {

        private final String topic;

        public HeartBeatTimerTask(String topic) {
            this.topic = topic;
        }

        @Override
        public void run() {
            SlothRemoteConsumer slothRemoteConsumer = SlothConsumerManager.get(this.topic);
            List<Integer> shouldConsumerPartitions = slothRemoteConsumer.heartbeat();
            List<Integer> currentConsumePartitions = slothRemoteConsumer.getCurrentConsumerPartitions();
            boolean consistence = slothRemoteConsumer.rebalanceCheck(shouldConsumerPartitions, currentConsumePartitions);
            if (!consistence) {
                log.info("topic:{} heartbeat, should consume partitions:{}, current consumer partitions:{} inconsistence!", this.topic, shouldConsumerPartitions, slothRemoteConsumer.topicPartitionConsumerMap);
                slothRemoteConsumer.rebalance(shouldConsumerPartitions, currentConsumePartitions);
            }
        }

    }

    private void rebalance(List<Integer> shouldConsumerPartitions, List<Integer> currentConsumePartitions) {
        //        List<Integer> topicPartitions = this.topicPartitionReBalanceRequest(this.consumerProperties);
//        Set<Integer> currentTopicPartitions = topicPartitionConsumerMap.keySet();
        //1.1、获取已经不需要消费的partition。

        //1.2、停止不需要消费的partition

        //2.1、获取缺少消费者的partition

        //2.2、启动消费者线程
    }

    private boolean rebalanceCheck(List<Integer> shouldConsumerPartitions, List<Integer> currentConsumePartitions) {
        List<Integer> a = shouldConsumerPartitions.stream().sorted().collect(Collectors.toList());
        List<Integer> b = currentConsumePartitions.stream().sorted().collect(Collectors.toList());
        return a.equals(b);
    }

    private List<Integer> getCurrentConsumerPartitions() {
        return new ArrayList<>(this.topicPartitionConsumerMap.keySet());
    }

    //----------------------------------------------

    public List<Integer> heartbeat() {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            Remoting.ConsumerHeartbeatRequest consumerHeartbeatRequest = Remoting.ConsumerHeartbeatRequest.newBuilder()
                    .setGroupName(groupName)
                    .setTopic(topic)
                    .setClientId(slothRemoteClient.getClientId())
                    .build();
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.CONSUMER_GROUP_HEARTBEAT)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(System.currentTimeMillis())
                    .data(consumerHeartbeatRequest.toByteArray())
                    .build();

            //send data
            slothRemoteClient.getChannel().writeAndFlush(dataPackage);

            DataPackage responseData = responseFuture.waitResponse();
            if (responseData == null) {
                log.warn("correlationId {} CONSUMER_GROUP_HEARTBEAT wait response null!", currentCorrelationId);
                return null;
            }
            byte[] data = responseData.getData();
            Remoting.ConsumerHeartbeatResult consumerHeartbeatResult = Remoting.ConsumerHeartbeatResult.parseFrom(data);
            if (consumerHeartbeatResult.getRetCode() == Remoting.RetCode.ERROR) {
                log.warn("correlationId {} CONSUMER_GROUP_HEARTBEAT got error! {} ", currentCorrelationId, consumerHeartbeatResult.getErrorInfo());
                return null;
            }

            return consumerHeartbeatResult.getPartitionsList();
        } catch (InterruptedException e) {
            log.warn("CONSUMER_GROUP_HEARTBEAT interrupted exception!" + e.getMessage());
        } catch (InvalidProtocolBufferException e) {
            log.warn("CONSUMER_GROUP_HEARTBEAT protobuf parse error!" + e.getMessage());
        } finally {
            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
        }
        return null;
    }


    public Long getConsumerOffset(String groupName, String topic, int topicPartition) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            Remoting.GetConsumerOffsetRequest consumerHeartbeatRequest = Remoting.GetConsumerOffsetRequest.newBuilder()
                    .setGroupName(groupName)
                    .setTopic(topic)
                    .setPartition(topicPartition)
                    .build();
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.GET_CONSUMER_GROUP_OFFSET)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(System.currentTimeMillis())
                    .data(consumerHeartbeatRequest.toByteArray())
                    .build();

            //send data
            slothRemoteClient.getChannel().writeAndFlush(dataPackage);

            DataPackage responseData = responseFuture.waitResponse();
            if (responseData == null) {
                log.warn("correlationId {} GET_CONSUMER_GROUP_OFFSET wait response null!", currentCorrelationId);
                return null;
            }
            byte[] data = responseData.getData();
            Remoting.GetConsumerOffsetResult getConsumerOffsetResult = Remoting.GetConsumerOffsetResult.parseFrom(data);
            if (getConsumerOffsetResult.getRetCode() == Remoting.RetCode.ERROR) {
                log.warn("correlationId {} GET_CONSUMER_GROUP_OFFSET got error! {} ", currentCorrelationId, getConsumerOffsetResult.getErrorInfo());
                return null;
            }
            return getConsumerOffsetResult.getOffset();
        } catch (InterruptedException e) {
            log.warn("GET_CONSUMER_GROUP_OFFSET interrupted exception!" + e.getMessage());
        } catch (InvalidProtocolBufferException e) {
            log.warn("GET_CONSUMER_GROUP_OFFSET protobuf parse error!" + e.getMessage());
        } finally {
            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
        }
        return null;
    }

    public Remoting.GetMessageResult fetchMessage(String topic, int partition, long offset) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            Remoting.GetMessageRequest getMessageRequest = Remoting.GetMessageRequest.newBuilder()
                    .setTopic(topic)
                    .setPartition(partition)
                    .setOffset(offset)
                    .build();
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.GET_MESSAGE)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(System.currentTimeMillis())
                    .data(getMessageRequest.toByteArray())
                    .build();

            //send data
            slothRemoteClient.getChannel().writeAndFlush(dataPackage);

            DataPackage responseData = responseFuture.waitResponse();
            if (responseData == null) {
                log.warn("correlationId {} GET_MESSAGE wait response null!", currentCorrelationId);
                return null;
            }
            byte[] data = responseData.getData();
            return Remoting.GetMessageResult.parseFrom(data);
        } catch (InterruptedException e) {
            log.warn("GET_MESSAGE interrupted exception!" + e.getMessage());
        } catch (InvalidProtocolBufferException e) {
            log.warn("GET_MESSAGE protobuf parse error!" + e.getMessage());
        } finally {
            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
        }
        return null;
    }

    public Remoting.SubmitConsumerOffsetResult submitOffset(String groupName, String topic, Integer partition, long offset) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            Remoting.SubmitConsumerOffsetRequest getMessageRequest = Remoting.SubmitConsumerOffsetRequest.newBuilder()
                    .setGroupName(groupName)
                    .setTopic(topic)
                    .setPartition(partition)
                    .setOffset(offset)
                    .build();
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.SUBMIT_CONSUMER_GROUP_OFFSET)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(System.currentTimeMillis())
                    .data(getMessageRequest.toByteArray())
                    .build();

            //send data
            slothRemoteClient.getChannel().writeAndFlush(dataPackage);

            DataPackage responseData = responseFuture.waitResponse();
            if (responseData == null) {
                log.warn("correlationId {} GET_MESSAGE wait response null!", currentCorrelationId);
                return null;
            }
            byte[] data = responseData.getData();
            return Remoting.SubmitConsumerOffsetResult.parseFrom(data);
        } catch (InterruptedException e) {
            log.warn("GET_MESSAGE interrupted exception!" + e.getMessage());
        } catch (InvalidProtocolBufferException e) {
            log.warn("GET_MESSAGE protobuf parse error!" + e.getMessage());
        } finally {
            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
        }
        return null;
    }

    public void removeTopicPartitionConsumerMapping(Integer partition) {
        this.topicPartitionConsumerMap.remove(partition);
    }
}
