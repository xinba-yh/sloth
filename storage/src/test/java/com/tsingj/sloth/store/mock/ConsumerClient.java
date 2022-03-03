package com.tsingj.sloth.store.mock;

import com.tsingj.sloth.store.StorageEngine;
import com.tsingj.sloth.store.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerClient {

    private Logger LOGGER = LoggerFactory.getLogger(ConsumerClient.class);

    private String topic;
    private int partitionMessageCount;
    private int partitionCount;
    private StorageEngine storageEngine;

    public ConsumerClient(String topic, int partitionMessageCount, int partitionCount, StorageEngine storageEngine) {
        this.topic = topic;
        this.partitionMessageCount = partitionMessageCount;
        this.storageEngine = storageEngine;
        this.partitionCount = partitionCount;
    }

    public void start() {
        final CountDownLatch countDownLatch = new CountDownLatch(partitionCount);
        long consumerStartTime = System.currentTimeMillis();
        AtomicLong success = new AtomicLong(0);
        for (int partition = 0; partition < partitionCount; partition++) {

            String helloWorld = "hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello wor";
            LOGGER.info("test message length:{}", helloWorld.length());

            Message message = new Message();
            message.setTopic(topic);
            message.setBody(helloWorld.getBytes(StandardCharsets.UTF_8));
            message.setPartition(partition);

            int finalPartition1 = partition;
            new Thread(() -> {
                for (int j = 0; j < partitionMessageCount; j++) {
                    int finalPartition = finalPartition1;
                    final int offset = j;

                    if (offset != 0 && (offset + 1) % 100 == 0) {
                        LOGGER.info("partition {} 已消费 {}。", finalPartition, offset + 1);
                    }
                    GetMessageResult getMessageResult = storageEngine.getMessage(topic, finalPartition, offset);
                    if (getMessageResult.getStatus() == GetMessageStatus.PARTITION_NO_MESSAGE || getMessageResult.getStatus() == GetMessageStatus.LOG_SEGMENT_NOT_FOUND) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {
                        }
                        getMessageResult = storageEngine.getMessage(topic, finalPartition, offset);
                        if (getMessageResult.getStatus() == GetMessageStatus.PARTITION_NO_MESSAGE || getMessageResult.getStatus() == GetMessageStatus.LOG_SEGMENT_NOT_FOUND) {
                            LOGGER.warn("partition {} consumer status {}", finalPartition, getMessageResult.getStatus());
                        }
                    } else if (getMessageResult.getStatus() != GetMessageStatus.FOUND) {
                        if (getMessageResult.getStatus() == GetMessageStatus.OFFSET_NOT_FOUND) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ignored) {
                            }
                            getMessageResult = storageEngine.getMessage(topic, finalPartition, offset);
                            if (getMessageResult.getStatus() != GetMessageStatus.OFFSET_NOT_FOUND) {
                                LOGGER.warn("partition {} consumer fail! {} {}", finalPartition, getMessageResult.getStatus(), getMessageResult.getErrorMsg());
                            }
                        }
                    } else {
                        success.addAndGet(1);
                    }
                }
                countDownLatch.countDown();
            }).start();
        }
        try {
            countDownLatch.await(10, TimeUnit.SECONDS);
            LOGGER.info("consumer cost:{}", System.currentTimeMillis() - consumerStartTime);
            LOGGER.info("success count {}.", success.get());
        } catch (InterruptedException ignored) {
        }

    }

}
