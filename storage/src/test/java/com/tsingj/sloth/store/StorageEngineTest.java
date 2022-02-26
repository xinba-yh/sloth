package com.tsingj.sloth.store;

import com.tsingj.sloth.store.properties.StorageProperties;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class StorageEngineTest {

    @Autowired
    private StorageEngine storageEngine;

    @Autowired
    private StorageProperties storageProperties;

    private static final String topic = "test-topic";

    private static final int count = 1000000;

    private static final int threadNum = 1;

    /**
     * 8 thread 10W -> 3S
     * 1 thread 80W -> 11S
     */
    @Test
    public void putMessageTest() {
        //------------------clear----------------------
        String dataPath = storageProperties.getDataPath();
        log.info("dataPath:{}", dataPath);
        File file = new File(dataPath);
        if (file.exists()) {
            file.delete();
        }

        long startTime = System.currentTimeMillis();

        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            int finalI = i;
            Thread thread = new Thread(() -> {
                //------------------test----------------------
                String helloWorld = "hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.";
                StopWatch sw = new StopWatch();
                Message message = Message.builder().topic(topic).partition(finalI).body(helloWorld.getBytes(StandardCharsets.UTF_8)).build();
                sw.start();
                for (int i1 = 0; i1 < count; i1++) {
                    PutMessageResult putMessageResult = storageEngine.putMessage(message);
                    if (putMessageResult.getStatus() != PutMessageStatus.OK) {
                        log.error("set error.{}", putMessageResult.getErrorMsg());
                    }
                }
                sw.stop();
                System.out.println("put count:" + count + ",cost:" + sw.getTotalTimeMillis());
                countDownLatch.countDown();
            });
            thread.start();
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("put count:" + count * threadNum + ", all cost:" + (System.currentTimeMillis() - startTime) + "ms");

    }

    @Test
    public void getMessageTest() {
        //mock data
        putMessageTest();
        try {
            Thread.sleep(10);
        } catch (InterruptedException ignored) {
        }

        //random get message
        int loopCount = 10000;
        StopWatch sw = new StopWatch();
        int[] random = this.random(loopCount);
        sw.start();
        for (int i = 0; i < loopCount; i++) {
            long offset = (i == 0) ? 0L : random[i];
            GetMessageResult result = storageEngine.getMessage(topic, 0, offset);
            if (result.getStatus() != GetMessageStatus.FOUND) {
                log.warn("get msg fail,{}:{}! ", result.getStatus(), result.getErrorMsg());
            }
        }
        sw.stop();
        log.info("data count {} , query {} times , cost:{}", count, loopCount, sw.getTotalTimeMillis());
    }

    @Test
    public void simpleDataPutGetTest() {
        String helloWorld = "hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.";
        Message message = Message.builder().topic(topic).partition(0).body(helloWorld.getBytes(StandardCharsets.UTF_8)).build();
        PutMessageResult putMessageResult1 = storageEngine.putMessage(message);
        System.out.println(putMessageResult1);
        Assert.isTrue(putMessageResult1.getStatus() == PutMessageStatus.OK);
        PutMessageResult putMessageResult2 = storageEngine.putMessage(message);
        System.out.println(putMessageResult2);
        Assert.isTrue(putMessageResult2.getStatus() == PutMessageStatus.OK);

        GetMessageResult getMessageResult1 = storageEngine.getMessage(topic, 0, 0L);
        System.out.println(getMessageResult1);
        Assert.isTrue(getMessageResult1.getStatus() == GetMessageStatus.FOUND);

        GetMessageResult getMessageResult2 = storageEngine.getMessage(topic, 0, 1L);
        Assert.isTrue(getMessageResult2.getStatus() == GetMessageStatus.FOUND);
    }

    private int[] random(int num) {
        int i = 1;
        Random random = new Random();
        int[] id = new int[num];
        id[0] = random.nextInt(count);
        while (i < num) {
            if (id[i] != random.nextInt(count)) {
                id[i] = random.nextInt(count);
            } else {
                continue;
            }
            i++;
        }
        Arrays.sort(id);
        System.out.println(Arrays.toString(id));
        return id;
    }

}
