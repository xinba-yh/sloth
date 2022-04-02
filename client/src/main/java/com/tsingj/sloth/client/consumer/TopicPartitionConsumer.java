package com.tsingj.sloth.client.consumer;

import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.remoting.message.Remoting;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yanghao
 */
@Slf4j
public class TopicPartitionConsumer implements Runnable {

    private final String topic;

    private final String groupName;

    private final Integer partition;

    private final Object lock = new Object();

    /**
     * 只需要保证线程可见行
     */
    private volatile long currentOffset;

    private volatile boolean running = true;

    /**
     * partition消费时，无消息可以消费状态拉取间隔
     */
    private final Integer consumeWhenNoMessageInterval;

    /**
     * partition消费时，默认错误休眠时间，default 200ms
     */
    private final Integer consumeWhenErrorSleepTimeDefault;

    /**
     * partition消费时，最大错误休眠时间，default 5000ms
     */
    private final Integer consumeWhenErrorSleepTimeMax;

    private final AtomicLong errorTimes = new AtomicLong(0);

    /**
     * partition消费时，默认listener执行错误休眠时间，default 1S
     * 实际休眠时长为：consumeWhenListenerErrorSleepTimeDefault * 失败次数
     */
    private final Integer consumeWhenListenerErrorSleepTimeDefault;

    /**
     * 消息失败最大次数
     */
    private final Integer consumeWhenListenerErrorMaxTimes;


    private final AtomicLong consumeFailTimes = new AtomicLong(0);


    public TopicPartitionConsumer(ConsumerProperties consumerProperties, Integer partition) {
        this.groupName = consumerProperties.getGroupName();
        this.topic = consumerProperties.getTopic();
        this.partition = partition;
        this.consumeWhenNoMessageInterval = consumerProperties.getConsumeWhenNoMessageInterval();
        this.consumeWhenErrorSleepTimeDefault = consumerProperties.getConsumeWhenErrorSleepTimeDefault();
        this.consumeWhenErrorSleepTimeMax = consumerProperties.getConsumeWhenErrorSleepTimeMax();
        this.consumeWhenListenerErrorSleepTimeDefault = consumerProperties.getConsumeWhenListenerErrorSleepTimeDefault();
        this.consumeWhenListenerErrorMaxTimes = consumerProperties.getConsumeWhenListenerErrorMaxTimes();
    }

    @Override
    public void run() {
        log.info("start consumer topic:{} partition:{} .", this.topic, this.partition);
        //1、询问broker，应该从哪里开始消费
        SlothRemoteConsumer slothConsumer = SlothConsumerManager.get(topic);
        Assert.notNull(slothConsumer, "topic:" + topic + " consumer is null!");
        Long consumerOffset = slothConsumer.getConsumerOffset(this.groupName, this.topic, this.partition);
        if (consumerOffset == null) {
            //定时心跳会check并再次拉起。
            slothConsumer.removeTopicPartitionConsumerMapping(this.partition);
            log.error("get topic:{} partition:{} consumerOffset fail!", this.topic, this.partition);
            return;
        }
        this.currentOffset = consumerOffset + 1;

        //2、开始消费
        try {
            while (running) {
                try {
                    //1、获取消息
                    Remoting.GetMessageResult getMessageResult = slothConsumer.fetchMessage(this.topic, this.partition, this.currentOffset);
                    if (getMessageResult == null || getMessageResult.getRetCode() == Remoting.GetMessageResult.RetCode.ERROR) {
                        this.errorSleep();
                    } else {
                        if (this.errorTimes.get() != 0) {
                            this.errorTimes.set(0);
                        }
                        //1.2、根据消息状态判断，如果没有可以消费的消息则休眠一段时间
                        if (getMessageResult.getRetCode() == Remoting.GetMessageResult.RetCode.NOT_FOUND) {
                            log.debug("fetch message topic:{} partition:{} offset:{}, got NOT_FOUND!", this.topic, this.partition, this.currentOffset);
                            this.waitMills(this.consumeWhenNoMessageInterval);
                        }//2、获取到消息，触发回调
                        else if (getMessageResult.getRetCode() == Remoting.GetMessageResult.RetCode.FOUND) {
                            Remoting.GetMessageResult.Message message = getMessageResult.getMessage();
                            ConsumerStatus consumerStatus = slothConsumer.getMessageListener().consumeMessage(message);
                            //2.1、用户消费成功,提交offset
                            if (consumerStatus == ConsumerStatus.SUCCESS) {
                                if (this.consumeFailTimes.get() != 0) {
                                    this.consumeFailTimes.set(0);
                                }
                                this.submitOffset(slothConsumer);
                            } else {
                                //2.2、用户消费失败，重试maxTimes后，提交offset，skip当前message，未来支持死信队列。
                                long currConsumeFailTimes = this.consumeFailTimes.incrementAndGet();
                                if (currConsumeFailTimes > this.consumeWhenListenerErrorMaxTimes) {
                                    log.warn("topic:{} partition:{} consume offset:{} fail maxTimes:{}, skip this offset!", this.topic, this.partition, this.currentOffset, this.consumeWhenListenerErrorMaxTimes);
                                    this.submitOffset(slothConsumer);
                                }
                                this.userConsumeFailSleep(currConsumeFailTimes);
                            }
                        } else {
                            log.error("unSupported RetCode：{},doNothing!", getMessageResult.getRetCode());
                        }
                    }

                } catch (Throwable e) {
                    log.warn("consume message got error:{}, errorTimes:{} !", e.getMessage(), errorTimes.get());
                    errorSleep();
                }
            }
        } finally {
            slothConsumer.removeTopicPartitionConsumerMapping(this.partition);
            log.warn("topic:{} partition:{} consumer quit!", this.topic, this.partition);
        }
    }

    private void submitOffset(SlothRemoteConsumer slothConsumer) {
        Remoting.SubmitConsumerOffsetResult submitConsumerOffsetResult = slothConsumer.submitOffset(this.groupName, this.topic, this.partition, this.currentOffset);
        if (submitConsumerOffsetResult == null) {
            log.warn("topic:{} partition:{} submit offset:{} may timeout or exception!", this.topic, this.partition, this.currentOffset);
            this.errorSleep();
        } else if (submitConsumerOffsetResult.getRetCode() == Remoting.RetCode.SUCCESS) {
            log.debug("topic:{} partition:{} submit offset:{} success!", this.topic, this.partition, this.currentOffset);
            this.currentOffset = this.currentOffset + 1;
        } else {
            log.warn("topic:{} partition:{} submit offset:{} fail! {}", this.topic, this.partition, this.currentOffset, submitConsumerOffsetResult.getErrorInfo());
            this.errorSleep();
        }
    }

    public void stop() {
        this.running = false;
        this.weekUp();
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

    /**
     * 错误休眠就好了，防止生产者weekUp继续出错。
     */
    public void errorSleep() {
        long currErrorTimes = this.errorTimes.incrementAndGet();
        long errorSleepTimeMill = Math.min(this.consumeWhenErrorSleepTimeDefault * currErrorTimes, this.consumeWhenErrorSleepTimeMax);
        try {
            Thread.sleep(errorSleepTimeMill);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * 用户消费失败休眠，防止生产者weekUp继续出错。
     */
    public void userConsumeFailSleep(long errorTimes) {
        long errorSleepTimeMill = this.consumeWhenListenerErrorSleepTimeDefault * errorTimes;
        try {
            Thread.sleep(errorSleepTimeMill);
        } catch (InterruptedException ignored) {
        }
    }

    public long getCurrentOffset() {
        return this.currentOffset;
    }

}
