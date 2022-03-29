package com.tsingj.sloth.client.consumer;

import org.springframework.util.Assert;

/**
 * @author yanghao
 */
public class TopicPartitionConsumer implements Runnable {

    private final String topic;

    private final Integer topicPartition;

    private volatile long currentOffset;

    private final Object lock = new Object();

    private volatile boolean running = true;


    public TopicPartitionConsumer(String topic, Integer topicPartition) {
        this.topic = topic;
        this.topicPartition = topicPartition;
    }

    @Override
    public void run() {
        //1、询问broker，应该从哪里开始消费
        SlothConsumer slothConsumer = SlothConsumerManager.getSlothConsumer(topic);
        Assert.notNull(slothConsumer, "topic:" + topic + " consumer is null!");
        slothConsumer.getConsumerOffset(topicPartition);
        this.currentOffset = 0L;
        //2、开始消费
        while (running) {
            long offset = currentOffset + 1;
            //1、获取消息
            slothConsumer.consumerMessage(offset);
            //1.2、根据消息状态判断，如果没有可以消费的消息则休眠一段时间
            synchronized (this.lock) {
                try {
                    this.lock.wait(200);
                } catch (InterruptedException ignored) {
                }
            }
            //2、触发回调

            //3.1、消费状态成功，提交消息Offset
            slothConsumer.submitOffset(offset);
            currentOffset++;
            //3.2、todo 消费状态失败。

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

}
