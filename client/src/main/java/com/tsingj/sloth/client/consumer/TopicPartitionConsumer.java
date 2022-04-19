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

    private final String consumeFromWhere;

    /**
     * 当前消费offset
     */
    private AtomicLong consumeOffset;

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
        this.consumeFromWhere = consumerProperties.getConsumeFromWhere();
        this.consumeWhenNoMessageInterval = consumerProperties.getConsumeWhenNoMessageInterval();
        this.consumeWhenErrorSleepTimeDefault = consumerProperties.getConsumeWhenErrorSleepTimeDefault();
        this.consumeWhenErrorSleepTimeMax = consumerProperties.getConsumeWhenErrorSleepTimeMax();
        this.consumeWhenListenerErrorSleepTimeDefault = consumerProperties.getConsumeWhenListenerErrorSleepTimeDefault();
        this.consumeWhenListenerErrorMaxTimes = consumerProperties.getConsumeWhenListenerErrorMaxTimes();
    }

    @Override
    public void run() {
        //wait init.
        while (!SlothConsumerManager.READY.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }
        //1、询问broker，应该从哪里开始消费
        SlothRemoteConsumer slothConsumer = SlothConsumerManager.get(topic);
        Assert.notNull(slothConsumer, "topic:" + topic + " consumer is null!");
        //注意：如果resetConsumeOffset失败，定时心跳会check并再次拉起。
        boolean success = this.resetConsumeOffset(slothConsumer);
        if (!success) {
            slothConsumer.removeTopicPartitionConsumerMapping(this.partition);
            log.error("reset topic:{} partition:{} consumeOffset fail!", this.topic, this.partition);
            return;
        }
        log.info("start consume topic:{} partition:{} offset {}.", this.topic, this.partition, this.consumeOffset);
        //2、开始消费
        try {
            while (running) {
                try {
                    //1、获取消息
                    log.debug("prepare consume topic:{} partition:{} offset {}.", this.topic, this.partition, this.consumeOffset);
                    Remoting.GetMessageResult getMessageResult = slothConsumer.fetchMessage(this.topic, this.partition, this.consumeOffset.get());
                    if (getMessageResult == null || getMessageResult.getRetCode() == Remoting.GetMessageResult.RetCode.ERROR) {
                        log.debug("consume topic:{} partition:{} offset {} fetchMessage fail!.", this.topic, this.partition, this.consumeOffset);
                        this.errorSleep();
                    } else {
                        if (this.errorTimes.get() != 0) {
                            this.errorTimes.set(0);
                        }
                        //1.2、根据消息状态判断，如果没有可以消费的消息则休眠一段时间
                        if (getMessageResult.getRetCode() == Remoting.GetMessageResult.RetCode.NOT_FOUND) {
                            Long maxOffset = slothConsumer.getMaxOffset(this.topic, this.partition);
                            if (maxOffset != null && maxOffset != -1 && this.consumeOffset.get() < maxOffset) {
                                //==null 通讯异常， == -1 代表没有logSegment，消费offset < maxOffset ！ 程序错误，或者log文件缺失
                                log.warn("Unexpected retCode NOT_FOUND，consume offset:{}, maxOffset:{}, skip this offset!", this.consumeOffset.get(), maxOffset);
                                this.consumeOffset.incrementAndGet();
                                this.waitMills(this.consumeWhenNoMessageInterval);
                            } else {
                                log.warn("fetch message topic:{} partition:{} offset:{}, got NOT_FOUND!", this.topic, this.partition, this.consumeOffset.get());
                                this.waitMills(this.consumeWhenNoMessageInterval);
                            }
                        }//2、获取到消息，触发回调
                        else if (getMessageResult.getRetCode() == Remoting.GetMessageResult.RetCode.FOUND) {
                            Remoting.GetMessageResult.Message message = getMessageResult.getMessage();
                            log.debug("consume topic:{} partition:{} offset {} prepare callback.", this.topic, this.partition, this.consumeOffset);
                            ConsumeStatus consumeStatus = slothConsumer.getMessageListener().consumeMessage(message);
                            //2.1、用户消费成功,提交offset
                            if (consumeStatus == ConsumeStatus.SUCCESS) {
                                if (this.consumeFailTimes.get() != 0) {
                                    this.consumeFailTimes.set(0);
                                }
                                this.submitOffset(slothConsumer);
                                log.debug("submit topic:{} partition:{} offset {}.", this.topic, this.partition, this.consumeOffset);
                            } else {
                                //2.2、用户消费失败，重试maxTimes后，提交offset，skip当前message，未来支持死信队列。
                                long currConsumeFailTimes = this.consumeFailTimes.incrementAndGet();
                                if (currConsumeFailTimes > this.consumeWhenListenerErrorMaxTimes) {
                                    log.warn("topic:{} partition:{} consume offset:{} fail maxTimes:{}, skip this offset!", this.topic, this.partition, this.consumeOffset.get(), this.consumeWhenListenerErrorMaxTimes);
                                    this.consumeOffset.incrementAndGet();
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

    private boolean resetConsumeOffset(SlothRemoteConsumer slothConsumer) {
        try {
            Long consumerOffset = slothConsumer.getConsumerOffset(this.groupName, this.topic, this.partition);
            if (consumerOffset == null) {
                return false;
            }
            //1、-1为没有提交过consume offset，EARLIEST获取minOffset，LATEST获取maxOffset。
            if (consumerOffset == -1) {
                //why EARLIEST需要获取最小的offset，而不是从0开始？ 因为可能已经超过了配置保留天数,minOffset不再是0。
                if (ConsumeFromWhere.EARLIEST.getType().equalsIgnoreCase(this.consumeFromWhere)) {
                    Long minOffset = slothConsumer.getMinOffset(this.topic, this.partition);
                    if (minOffset == null) {
                        return false;
                    }
                    this.consumeOffset = new AtomicLong(minOffset);
                } else if (ConsumeFromWhere.LATEST.getType().equalsIgnoreCase(this.consumeFromWhere)) {
                    Long maxOffset = slothConsumer.getMaxOffset(this.topic, this.partition);
                    if (maxOffset == null) {
                        return false;
                    }
                    this.consumeOffset = new AtomicLong(maxOffset + 1);
                } else {
                    log.error("unsupported consumeFromWhere:{}!", this.consumeFromWhere);
                    return false;
                }
            }
            //2、提交过consume offset,则从提交的consumerOffset+1开始
            else {
                this.consumeOffset = new AtomicLong(consumerOffset + 1);
            }
            return true;
        } catch (Throwable e) {
            log.error("reset consume offset fail!", e);
            return false;
        }
    }


    private void submitOffset(SlothRemoteConsumer slothConsumer) {
        Remoting.SubmitConsumerOffsetResult submitConsumerOffsetResult = slothConsumer.submitOffset(this.groupName, this.topic, this.partition, this.consumeOffset.get());
        if (submitConsumerOffsetResult == null) {
            log.warn("topic:{} partition:{} submit offset:{} may timeout or exception!", this.topic, this.partition, this.consumeOffset.get());
            this.errorSleep();
        } else if (submitConsumerOffsetResult.getRetCode() == Remoting.RetCode.SUCCESS) {
            log.debug("topic:{} partition:{} submit offset:{} success!", this.topic, this.partition, this.consumeOffset.get());
            this.consumeOffset.incrementAndGet();
        } else {
            log.warn("topic:{} partition:{} submit offset:{} fail! {}", this.topic, this.partition, this.consumeOffset.get(), submitConsumerOffsetResult.getErrorInfo());
            this.errorSleep();
        }
    }

    public void stop() {
        this.running = false;
        this.weekUp();
    }

    public void weekUp() {
        // TODO: 2022/4/4  add AtomicBoolean hasNotified = new AtomicBoolean(false); check.
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

    public long getCurrentConsumeOffset() {
        return this.consumeOffset.get();
    }

}
