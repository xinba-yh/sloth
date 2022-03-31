package com.tsingj.sloth.client.consumer;

import com.tsingj.sloth.remoting.message.Remoting;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

/**
 * @author yanghao
 */
@Slf4j
public class TopicPartitionConsumer implements Runnable {

    private final String topic;

    private final String groupName;

    private final Integer partition;

    private final Object lock = new Object();

    private volatile long currentOffset;

    private volatile boolean running = true;


    public TopicPartitionConsumer(String groupName, String topic, Integer partition) {
        this.groupName = groupName;
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public void run() {
        Long consumerOffset;
        //1、询问broker，应该从哪里开始消费
        SlothRemoteConsumer slothConsumer = SlothConsumerManager.get(topic);
        Assert.notNull(slothConsumer, "topic:" + topic + " consumer is null!");
        consumerOffset = slothConsumer.getConsumerOffset(groupName, topic, partition);
        if (consumerOffset == null) {
            //定时心跳会check并再次拉起。
            slothConsumer.removeTopicPartitionConsumerMapping(this.partition);
            log.error("get topic:{} consumerOffset fail!", topic);
            return;
        }
        try {
            long currentOffset = consumerOffset + 1;
            //2、开始消费
            while (running) {
                try {
                    //1、获取消息
                    Remoting.GetMessageResult getMessageResult = slothConsumer.fetchMessage(topic, partition, currentOffset);
                    if (getMessageResult == null || getMessageResult.getRetCode() == Remoting.GetMessageResult.RetCode.ERROR) {
                        log.error("fetch message fail! may timeout or exception! {}", getMessageResult != null ? getMessageResult.getErrorInfo() : "");
                        this.waitMills(1000);
                        continue;
                    }
                    //1.2、根据消息状态判断，如果没有可以消费的消息则休眠一段时间
                    if (getMessageResult.getRetCode() == Remoting.GetMessageResult.RetCode.NOT_FOUND) {
                        log.warn("fetch message topic:{} partition:{} offset:{}, got not found()!", this.topic, this.partition, currentOffset);
                        this.waitMills(500);
                        continue;
                    }

                    //2、获取到消息，触发回调
                    if (getMessageResult.getRetCode() == Remoting.GetMessageResult.RetCode.FOUND) {
                        Remoting.GetMessageResult.Message message = getMessageResult.getMessage();
                        ConsumerStatus consumerStatus = slothConsumer.getMessageListener().consumeMessage(message);
                        if (consumerStatus == ConsumerStatus.SUCCESS) {
                            //3.1、消费状态成功，提交消息Offset
                            Remoting.SubmitConsumerOffsetResult submitConsumerOffsetResult = slothConsumer.submitOffset(this.groupName, this.topic, this.partition, currentOffset);
                            if (submitConsumerOffsetResult == null) {
                                log.warn("topic:{} partition:{} submit offset:{}  may timeout or exception!", this.topic, this.partition, currentOffset);
                                this.waitMills(1000);
                            } else if (submitConsumerOffsetResult.getRetCode() == Remoting.RetCode.SUCCESS) {
                                this.currentOffset = this.currentOffset + 1;
                                log.info("topic:{} partition:{} submit offset:{} success!", this.topic, this.partition, currentOffset);
                            } else {
                                log.error("topic:{} partition:{} submit offset:{} fail! {}", this.topic, this.partition, currentOffset, submitConsumerOffsetResult.getErrorInfo());
                                this.waitMills(1000);
                            }
                        }
                    } else {
                        log.warn("unSupported RetCode：{},doNothing!", getMessageResult.getRetCode());
                    }
                    //3.2、todo 消费状态失败。
                } catch (Throwable e) {
                    this.waitMills(1000);
                }
            }
        } finally {
            slothConsumer.removeTopicPartitionConsumerMapping(this.partition);
            log.warn("topic:{} partition:{} consumer quit!", this.topic, this.partition);
        }

    }

    public void stop() {
        this.running = false;
    }

    public void weekUp() {
        synchronized (this.lock) {
            this.lock.notify();
        }
    }

    public void waitMills(long timeMills) {
        synchronized (this.lock) {
            try {
                this.lock.wait(timeMills);
            } catch (InterruptedException ignored) {
            }
        }
    }

    public long getCurrentOffset() {
        return this.currentOffset;
    }

}
