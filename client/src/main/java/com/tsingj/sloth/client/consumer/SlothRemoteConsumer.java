package com.tsingj.sloth.client.consumer;

import com.tsingj.sloth.client.SlothRemoteClient;
import com.tsingj.sloth.client.SlothRemoteClientSingleton;
import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.client.springsupport.RemoteProperties;
import com.tsingj.sloth.common.threadpool.TaskThreadFactory;
import com.tsingj.sloth.common.threadpool.fixed.FixedThreadPoolExecutor;
import com.tsingj.sloth.remoting.message.Remoting;
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

    private RemoteProperties remoteProperties;

    private ConsumerProperties consumerProperties;

    private SlothRemoteClient slothRemoteClient;

    private MessageListener messageListener;

    private ExecutorService executorService;

    private ScheduledExecutorService scheduledExecutorService;


    public void setConsumerProperties(ConsumerProperties consumerProperties) {
        this.consumerProperties = consumerProperties;
    }

    public void setRemoteProperties(RemoteProperties remoteProperties) {
        this.remoteProperties = remoteProperties;
    }

    public String getTopic() {
        return topic;
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
        this.slothRemoteClient = SlothRemoteClientSingleton.getInstance(this.remoteProperties);
        this.topic = this.consumerProperties.getTopic();
        this.groupName = this.consumerProperties.getGroupName();
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new TaskThreadFactory("consumer-hb-"));
        this.executorService = new FixedThreadPoolExecutor(consumerProperties.getMaxConsumePartitions(), consumerProperties.getMaxConsumePartitions(), 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(consumerProperties.getMaxConsumePartitions()), new TaskThreadFactory("consume-part"), new ThreadPoolExecutor.AbortPolicy());
        //注册自身
        SlothConsumerManager.register(this);
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
        this.scheduledExecutorService.scheduleAtFixedRate(new HeartBeatAndReBalanceCheckTimerTask(this), 3, 3, TimeUnit.SECONDS);
    }

    public void destroy() {
        log.info("sloth consumer {} destroy.", this.topic);
        this.scheduledExecutorService.shutdown();
        SlothConsumerManager.unregister(this.topic);
    }

    /**
     * 1、定时心跳
     * 2、重平衡检查
     */
    @Slf4j
    public static class HeartBeatAndReBalanceCheckTimerTask extends TimerTask {

        private final SlothRemoteConsumer consumer;

        public HeartBeatAndReBalanceCheckTimerTask(SlothRemoteConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            log.trace("run heartbeat timer task.");
            try {
                consumer.heartBeatAndReBalanceCheck();
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

    //-----------------------remote call-----------------------

    public List<Integer> heartbeat() {
        return this.slothRemoteClient.heartbeat(this.groupName, this.topic, consumerProperties.getMaxConsumePartitions());
    }

    public Long getConsumerOffset(String groupName, String topic, int partition) {
        return this.slothRemoteClient.getConsumerOffset(groupName, topic, partition);
    }

    public Long getMinOffset(String topic, Integer partition) {
        return this.slothRemoteClient.getMinOffset(topic, partition);
    }

    public Long getMaxOffset(String topic, Integer partition) {
        return this.slothRemoteClient.getMaxOffset(topic, partition);
    }


    public Remoting.GetMessageResult fetchMessage(String topic, int partition, long offset) {
        return this.slothRemoteClient.fetchMessage(topic, partition,offset);
    }

    public Remoting.SubmitConsumerOffsetResult submitOffset(String groupName, String topic, Integer partition, long offset) {
        return this.slothRemoteClient.submitOffset(groupName,topic, partition,offset);
    }

}
