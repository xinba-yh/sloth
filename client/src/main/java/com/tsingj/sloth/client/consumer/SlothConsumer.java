package com.tsingj.sloth.client.consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.client.RemoteCorrelationManager;
import com.tsingj.sloth.client.SlothClient;
import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author yanghao
 */
@Slf4j
public class SlothConsumer extends SlothClient {

    private final ConsumerProperties consumerProperties;

    public SlothConsumer(SlothClientProperties clientProperties, ConsumerProperties consumerProperties) {
        this.clientProperties = clientProperties;
        this.consumerProperties = consumerProperties;
        this.pollName = "sloth-producer";
    }

    /**
     * partition - consumer线程对应关系
     */
    private final ConcurrentHashMap<Integer, TopicPartitionConsumer> topicPartitionConsumerMap = new ConcurrentHashMap<>();

    /**
     * 当前channel clientId
     */
    private String clientId;

    private String topic;

    private String groupName;


    public void start() {
        this.initConnect();
        this.clientId = UUID.randomUUID().toString();
        this.topic = this.consumerProperties.getTopic();
        this.groupName = this.consumerProperties.getGroupName();
        //1、立即发送一次heartbeat，并与建立clientId与channel的绑定关系（client - server）。
        this.heartbeat();
        //2、获取应该消费的partition。
        List<Integer> topicPartitions = this.topicPartitionReBalanceRequest(this.consumerProperties);
        //3、TopicPartitionConsumerManager
        if (topicPartitions != null && topicPartitions.size() > 0) {
            for (Integer topicPartition : topicPartitions) {
                TopicPartitionConsumer topicPartitionConsumer = new TopicPartitionConsumer(this.topic, topicPartition);
                new Thread(topicPartitionConsumer).start();
                topicPartitionConsumerMap.put(topicPartition, topicPartitionConsumer);
            }
        }
        log.info("sloth topic {} consumer {} partitions {} init done.", this.topic, clientId, topicPartitions);
    }

    public void close() {
        this.closeConnect();
        log.info("sloth consumer {} destroy.", this.topic);
    }


    //----------------------------------------------


    private List<Integer> topicPartitionReBalanceRequest(ConsumerProperties clientProperties) {
        return null;
    }

    private void heartbeat() {

    }

    private void reBalanceTopicPartition() {
        List<Integer> topicPartitions = this.topicPartitionReBalanceRequest(this.consumerProperties);
        Set<Integer> currentTopicPartitions = topicPartitionConsumerMap.keySet();
        //1.1、获取已经不需要消费的partition。

        //1.2、停止不需要消费的partition

        //2.1、获取缺少消费者的partition

        //2.2、启动消费者线程

    }

    public void getConsumerOffset(int topicPartition) {
//        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
//        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
//        //add 关联关系，handler或者超时的定时任务将会清理。
//        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
//        try {
//            DataPackage dataPackage = DataPackage.builder()
//                    .magicCode(ProtocolConstants.MAGIC_CODE)
//                    .version(ProtocolConstants.VERSION)
//                    .command(ProtocolConstants.Command.SEND_MESSAGE)
//                    .requestType(ProtocolConstants.RequestType.SYNC)
//                    .correlationId(currentCorrelationId)
//                    .timestamp(System.currentTimeMillis())
//                    .data(message.toByteArray())
//                    .build();
//
//            //send data
//            this.channel.writeAndFlush(dataPackage);
//
//            DataPackage responseData = responseFuture.waitResponse();
//            if (responseData == null) {
//                log.warn("correlationId {} wait response null!", currentCorrelationId);
//                return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("receive data null!").build();
//            }
//            byte[] data = responseData.getData();
//            return Remoting.SendResult.parseFrom(data);
//        } catch (InterruptedException e) {
//            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.TIMEOUT).build();
//        } catch (InvalidProtocolBufferException e) {
//            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("protobuf parse error!" + e.getMessage()).build();
//        } finally {
//            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
//        }
    }

    public void consumerMessage(long offset) {

    }

    public void submitOffset(long offset) {

    }

}
