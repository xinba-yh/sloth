package com.tsingj.sloth.client.consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.client.RemoteCorrelationManager;
import com.tsingj.sloth.client.SlothRemoteClient;
import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.common.threadpool.TaskThreadFactory;
import com.tsingj.sloth.common.threadpool.fixed.FixedThreadPoolExecutor;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.*;
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

    private final ExecutorService executorService;

    private final ScheduledExecutorService scheduledExecutorService;

    public SlothRemoteConsumer(SlothClientProperties clientProperties, ConsumerProperties consumerProperties, SlothRemoteClient slothRemoteClient) {
        this.clientProperties = clientProperties;
        this.consumerProperties = consumerProperties;
        this.slothRemoteClient = slothRemoteClient;
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new TaskThreadFactory("consumer-hb-"));
        this.executorService = new FixedThreadPoolExecutor(consumerProperties.getMaxConsumePartitions(), consumerProperties.getMaxConsumePartitions(), 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(consumerProperties.getMaxConsumePartitions()), new TaskThreadFactory("consume-part"), new ThreadPoolExecutor.AbortPolicy());
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
        String consumeFromWhere = this.consumerProperties.getConsumeFromWhere();
        Assert.isTrue(StringUtils.hasLength(consumeFromWhere) && (consumeFromWhere.equalsIgnoreCase(ConsumeFromWhere.EARLIEST.getType()) || consumeFromWhere.equalsIgnoreCase(ConsumeFromWhere.LATEST.getType())), "please check consumer properties consumeFromWhere, " + consumeFromWhere + " unsupported !");
        //1.1、立即发送一次heartbeat，并与建立clientId与channel的绑定关系（client - server）。
        //1.2、响应分配好的partition（可能被截断）
        List<Integer> topicPartitions = this.heartbeat();
        //2、TopicPartitionConsumerManager
        if (topicPartitions != null && topicPartitions.size() > 0) {
            for (Integer topicPartition : topicPartitions) {
                TopicPartitionConsumer topicPartitionConsumer = new TopicPartitionConsumer(this.consumerProperties, topicPartition);
                executorService.execute(topicPartitionConsumer);
                topicPartitionConsumerMap.put(topicPartition, topicPartitionConsumer);
            }
            log.info("sloth topic {} consumer {} partitions {} init done.", this.topic, slothRemoteClient.getClientId(), topicPartitions);
        }
        //3、启动心跳和重平衡检查定时任务
        this.scheduledExecutorService.scheduleAtFixedRate(new HeartBeatAndReBalanceCheckTimerTask(this.topic), 3, 3, TimeUnit.SECONDS);
    }

    public void destroy() {
        log.info("sloth consumer {} destroy.", this.topic);
        this.scheduledExecutorService.shutdown();
    }

    /**
     * 1、定时心跳
     * 2、重平衡检查
     */
    @Slf4j
    public static class HeartBeatAndReBalanceCheckTimerTask extends TimerTask {

        private final String topic;

        public HeartBeatAndReBalanceCheckTimerTask(String topic) {
            this.topic = topic;
        }

        @Override
        public void run() {
            log.trace("run heartbeat timer task.");
            try {
                SlothRemoteConsumer slothRemoteConsumer = SlothConsumerManager.get(this.topic);
                slothRemoteConsumer.heartBeatAndReBalanceCheck();
            } catch (Throwable e) {
                log.debug("heartBeatAndReBalanceCheckTimerTask run fail! {}", e.getMessage());
            }
            log.trace("run heartbeat timer task done.");
        }

    }

    public void heartBeatAndReBalanceCheck() {
        List<Integer> shouldConsumerPartitions = this.heartbeat();
        if (shouldConsumerPartitions == null) {
            return;
        }
        this.reBalanceCheck(shouldConsumerPartitions);
    }

    public void reBalanceCheck(List<Integer> shouldConsumerPartitions) {
        List<Integer> currentConsumePartitions = this.getCurrentConsumerPartitions();
        boolean consistence = shouldConsumerPartitions.equals(currentConsumePartitions);
        if (!consistence) {
            log.info("topic:{} reBalanceCheck, should consume partitions:{}, current consumer partitions:{} inconsistencies!", this.topic, shouldConsumerPartitions, currentConsumePartitions);
            this.reBalance(shouldConsumerPartitions, currentConsumePartitions);
        }
    }

    public void reBalance(List<Integer> shouldConsumerPartitions, List<Integer> currentConsumePartitions) {
        //1.1、获取已经不需要消费的partition。
        for (Integer currentConsumePartition : currentConsumePartitions) {
            //1.2、停止不需要消费的partition，当前消费的partition不在应该消费的partitions中，则停止消费。
            if (!shouldConsumerPartitions.contains(currentConsumePartition)) {
                TopicPartitionConsumer topicPartitionConsumer = this.topicPartitionConsumerMap.get(currentConsumePartition);
                if (topicPartitionConsumer == null) {
                    log.warn("try stop topic:{} partition:{} consumer,but now not exist!", this.topic, currentConsumePartition);
                    continue;
                }
                topicPartitionConsumer.stop();
                topicPartitionConsumer.weekUp();
                this.topicPartitionConsumerMap.remove(currentConsumePartition);
            }
        }

        //2.1、获取缺少消费者的partition
        for (Integer shouldConsumerPartition : shouldConsumerPartitions) {
            //2.2、新增缺少消费者的partition消费者线程
            if (!currentConsumePartitions.contains(shouldConsumerPartition)) {
                TopicPartitionConsumer topicPartitionConsumer = new TopicPartitionConsumer(this.consumerProperties, shouldConsumerPartition);
                executorService.execute(topicPartitionConsumer);
                topicPartitionConsumerMap.put(shouldConsumerPartition, topicPartitionConsumer);
            }
        }
    }

    public List<Integer> getCurrentConsumerPartitions() {
        //解决hash无序导致的partition乱序问题
        List<Integer> currentConsumerPartitions = new ArrayList<>(this.topicPartitionConsumerMap.keySet());
        return currentConsumerPartitions.parallelStream().sorted().collect(Collectors.toList());
    }

    public TopicPartitionConsumer getTopicPartitionConsumer(Integer partition) {
        return this.topicPartitionConsumerMap.get(partition);
    }


    public void removeTopicPartitionConsumerMapping(Integer partition) {
        this.topicPartitionConsumerMap.remove(partition);
    }

    //----------------------------------------------

    public List<Integer> heartbeat() {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        log.debug("prepare heartbeat currentCorrelationId:{}.", currentCorrelationId);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            Remoting.ConsumerHeartbeatRequest consumerHeartbeatRequest = Remoting.ConsumerHeartbeatRequest.newBuilder()
                    .setGroupName(groupName)
                    .setTopic(topic)
                    .setClientId(this.slothRemoteClient.getClientId())
                    .build();
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.CONSUMER_GROUP_HEARTBEAT)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(SystemClock.now())
                    .data(consumerHeartbeatRequest.toByteArray())
                    .build();

            //send data
            this.slothRemoteClient.getChannel().writeAndFlush(dataPackage);

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

            List<Integer> topicPartitions = consumerHeartbeatResult.getPartitionsList();
            //根据配置的maxConsumePartitions截断broker分配的partitions
            Integer maxConsumePartitions = this.consumerProperties.getMaxConsumePartitions();
            if (topicPartitions.size() > maxConsumePartitions) {
                topicPartitions = topicPartitions.parallelStream().limit(maxConsumePartitions).collect(Collectors.toList());
            }
            return topicPartitions;
        } catch (InterruptedException e) {
            log.warn("CONSUMER_GROUP_HEARTBEAT interrupted exception!" + e.getMessage());
        } catch (InvalidProtocolBufferException e) {
            log.warn("CONSUMER_GROUP_HEARTBEAT protobuf parse error!" + e.getMessage());
        } finally {
            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
            log.debug("send heartbeat done currentCorrelationId:{}.", currentCorrelationId);
        }
        return null;
    }

    public Long getConsumerOffset(String groupName, String topic, int partition) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            Remoting.GetConsumerOffsetRequest consumerHeartbeatRequest = Remoting.GetConsumerOffsetRequest.newBuilder()
                    .setGroupName(groupName)
                    .setTopic(topic)
                    .setPartition(partition)
                    .build();
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.GET_CONSUMER_GROUP_OFFSET)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(SystemClock.now())
                    .data(consumerHeartbeatRequest.toByteArray())
                    .build();

            //send data
            this.slothRemoteClient.getChannel().writeAndFlush(dataPackage);

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

    public Long getMinOffset(String topic, Integer partition) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            Remoting.GetOffsetRequest getOffsetRequest = Remoting.GetOffsetRequest.newBuilder()
                    .setTopic(topic)
                    .setPartition(partition)
                    .build();
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.GET_MIN_OFFSET)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(SystemClock.now())
                    .data(getOffsetRequest.toByteArray())
                    .build();

            //send data
            this.slothRemoteClient.getChannel().writeAndFlush(dataPackage);

            DataPackage responseData = responseFuture.waitResponse();
            if (responseData == null) {
                log.warn("correlationId {} GET_MIN_OFFSET wait response null!", currentCorrelationId);
                return null;
            }
            byte[] data = responseData.getData();
            Remoting.GetOffsetResult getOffsetResult = Remoting.GetOffsetResult.parseFrom(data);
            if (getOffsetResult.getRetCode() == Remoting.RetCode.ERROR) {
                log.warn("correlationId {} GET_MIN_OFFSET got error! {} ", currentCorrelationId, getOffsetResult.getErrorInfo());
                return null;
            }
            return getOffsetResult.getOffset();
        } catch (InterruptedException e) {
            log.warn("GET_MIN_OFFSET interrupted exception!" + e.getMessage());
        } catch (InvalidProtocolBufferException e) {
            log.warn("GET_MIN_OFFSET protobuf parse error!" + e.getMessage());
        } finally {
            RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
        }
        return null;
    }

    public Long getMaxOffset(String topic, Integer partition) {
        long currentCorrelationId = RemoteCorrelationManager.CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId, this.clientProperties.getConnect().getOnceTalkTimeout());
        //add 关联关系，handler或者超时的定时任务将会清理。
        RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            Remoting.GetOffsetRequest getOffsetRequest = Remoting.GetOffsetRequest.newBuilder()
                    .setTopic(topic)
                    .setPartition(partition)
                    .build();
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.GET_MAX_OFFSET)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(SystemClock.now())
                    .data(getOffsetRequest.toByteArray())
                    .build();

            //send data
            this.slothRemoteClient.getChannel().writeAndFlush(dataPackage);

            DataPackage responseData = responseFuture.waitResponse();
            if (responseData == null) {
                log.warn("correlationId {} GET_MAX_OFFSET wait response null!", currentCorrelationId);
                return null;
            }
            byte[] data = responseData.getData();
            Remoting.GetOffsetResult getOffsetResult = Remoting.GetOffsetResult.parseFrom(data);
            if (getOffsetResult.getRetCode() == Remoting.RetCode.ERROR) {
                log.warn("correlationId {} GET_MAX_OFFSET got error! {} ", currentCorrelationId, getOffsetResult.getErrorInfo());
                return null;
            }
            return getOffsetResult.getOffset();
        } catch (InterruptedException e) {
            log.warn("GET_MAX_OFFSET interrupted exception!" + e.getMessage());
        } catch (InvalidProtocolBufferException e) {
            log.warn("GET_MAX_OFFSET protobuf parse error!" + e.getMessage());
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
                    .timestamp(SystemClock.now())
                    .data(getMessageRequest.toByteArray())
                    .build();

            //send data
            this.slothRemoteClient.getChannel().writeAndFlush(dataPackage);

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
                    .timestamp(SystemClock.now())
                    .data(getMessageRequest.toByteArray())
                    .build();

            //send data
            this.slothRemoteClient.getChannel().writeAndFlush(dataPackage);

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

}
