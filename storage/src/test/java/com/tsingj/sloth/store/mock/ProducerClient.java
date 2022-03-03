package com.tsingj.sloth.store.mock;

import com.tsingj.sloth.store.StorageEngine;
import com.tsingj.sloth.store.pojo.Message;
import com.tsingj.sloth.store.pojo.PutMessageResult;
import com.tsingj.sloth.store.pojo.PutMessageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerClient {

    private Logger LOGGER = LoggerFactory.getLogger(ProducerClient.class);

    private String topic;
    private int partitionMessageCount;
    private int partitionCount;
    private StorageEngine storageEngine;


    public ProducerClient(String topic, int partitionMessageCount, int partitionCount, StorageEngine storageEngine) {
        this.topic = topic;
        this.partitionMessageCount = partitionMessageCount;
        this.partitionCount = partitionCount;
        this.storageEngine = storageEngine;
    }

    public void start() {
        CountDownLatch countDownLatch = new CountDownLatch(partitionCount);
        ExecutorService executorService = Executors.newFixedThreadPool(8);

        long producerStartTime = System.currentTimeMillis();
        int[] partitions = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        for (int partition : partitions) {
            String helloWorld = "hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello wor";
            LOGGER.info("test message length:{}", helloWorld.length());

            Message message = new Message();
            message.setTopic(topic);
            message.setBody(helloWorld.getBytes(StandardCharsets.UTF_8));
            message.setPartition(partition);

            new Thread(() -> {
                for (int j = 0; j < partitionMessageCount; j++) {
                    PutMessageResult putMessageResult = storageEngine.putMessage(message);
                    if (putMessageResult.getStatus() != PutMessageStatus.OK) {
                        LOGGER.warn("putMessage fail! {}", putMessageResult.getErrorMsg());
                    } else {
//                        LOGGER.info("partition:{} respOffset:{}", message.getPartition(), putMessageResult.getOffset());
                    }
                }
                countDownLatch.countDown();
            }).start();

        }
        try {
            countDownLatch.await(10, TimeUnit.SECONDS);
            LOGGER.info("producer count:{} cost:{}", partitionMessageCount * 9, System.currentTimeMillis() - producerStartTime);
            executorService.shutdown();
        } catch (InterruptedException ignored) {
        }

    }


}