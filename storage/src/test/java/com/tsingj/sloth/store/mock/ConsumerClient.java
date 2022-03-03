package com.tsingj.sloth.store.mock;

import com.tsingj.sloth.store.StorageEngine;
import com.tsingj.sloth.store.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        final CountDownLatch countDownLatch = new CountDownLatch(partitionMessageCount * partitionCount);
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        AtomicLong success = new AtomicLong(0);
        for (int partition = 0; partition < partitionCount; partition++) {

            String helloWorld = "hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello wor";
            LOGGER.info("test message length:{}", helloWorld.length());

            Message message = new Message();
            message.setTopic(topic);
            message.setBody(helloWorld.getBytes(StandardCharsets.UTF_8));
            message.setPartition(partition);

            for (int j = 0; j < partitionMessageCount; j++) {
                int finalPartition = partition;
                final int offset = j;
                executorService.execute(() -> {
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
//                        LOGGER.info("partition:{} query:{} got:{}", finalPartition, offset, getMessageResult.getMessage().getOffset());
                    }
                    countDownLatch.countDown();
                });
            }
        }
        try {
            countDownLatch.await(10, TimeUnit.SECONDS);
            executorService.shutdown();
            LOGGER.info("success count {}.", success.get());
        } catch (InterruptedException ignored) {
        }

    }

}
