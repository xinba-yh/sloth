package com.tsingj.sloth.client.consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.client.RemoteCorrelationManager;
import com.tsingj.sloth.client.SlothRemoteClient;
import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


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


    public void init() {
        this.topic = this.consumerProperties.getTopic();
        this.groupName = this.consumerProperties.getGroupName();
        //1.1、立即发送一次heartbeat，并与建立clientId与channel的绑定关系（client - server）。
        //1.2、响应分配好的partition
        List<Integer> topicPartitions = this.heartbeat(groupName, topic, slothRemoteClient.getClientId());
        //2、TopicPartitionConsumerManager
        if (topicPartitions != null && topicPartitions.size() > 0) {
            for (Integer topicPartition : topicPartitions) {
                TopicPartitionConsumer topicPartitionConsumer = new TopicPartitionConsumer(this.groupName, this.topic, topicPartition);
                new Thread(topicPartitionConsumer).start();
                topicPartitionConsumerMap.put(topicPartition, topicPartitionConsumer);
            }
        }
        //3、启动心跳线程

        log.info("sloth topic {} consumer {} partitions {} init done.", this.topic, slothRemoteClient.getClientId(), topicPartitions);
    }

    public void destroy() {
        log.info("sloth consumer {} destroy.", this.topic);
    }


    //----------------------------------------------

    public List<Integer> heartbeat(String groupName, String topic, String clientId) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            Remoting.ConsumerHeartbeatRequest consumerHeartbeatRequest = Remoting.ConsumerHeartbeatRequest.newBuilder()
                    .setGroupName(groupName)
                    .setTopic(topic)
                    .setClientId(clientId)
                    .build();
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.CONSUMER_GROUP_HEARTBEAT)
                    .requestType(ProtocolConstants.RequestType.SYNC)
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

    private void reBalanceTopicPartition() {
//        List<Integer> topicPartitions = this.topicPartitionReBalanceRequest(this.consumerProperties);
//        Set<Integer> currentTopicPartitions = topicPartitionConsumerMap.keySet();
        //1.1、获取已经不需要消费的partition。

        //1.2、停止不需要消费的partition

        //2.1、获取缺少消费者的partition

        //2.2、启动消费者线程

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

    public void consumerMessage(String groupName, String topic, long offset) {

    }

    public void submitOffset(String groupName, String topic, long offset) {

    }

}
