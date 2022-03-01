package com.tsingj.sloth.store;

import com.tsingj.sloth.store.log.IndexEntry;
import com.tsingj.sloth.store.log.OffsetIndex;
import com.tsingj.sloth.store.pojo.*;
import com.tsingj.sloth.store.properties.StorageProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class StorageEngineTest {

    private static final Logger log = LoggerFactory.getLogger(StorageEngineTest.class);

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
        long startTime = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            int finalI = i;
            Thread thread = new Thread(() -> {
                //------------------test----------------------
                String helloWorld = "hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.";
                StopWatch sw = new StopWatch();
                Message message = new Message();
                message.setTopic(topic);
                message.setPartition(finalI);
                message.setBody(helloWorld.getBytes(StandardCharsets.UTF_8));
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
            } else {
                if (offset != result.getMessage().getOffset()) {
                    log.warn("get msg fail，query:{}，got:{}! ", offset, result.getMessage().getOffset());
                }
//                log.info("get message offset:{} message:{}",offset,result.getMessage());
            }
        }
        sw.stop();
        log.info("data count {} , query {} times , cost:{}", count, loopCount, sw.getTotalTimeMillis());
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

    @Test
    public void offsetIndexReaderTest() throws IOException {
        File file = new File(storageProperties.getDataPath() + File.separator + topic + File.separator + 0 + File.separator + "00000000000000000000.index");
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

        for (int i = 0; i < file.length() / 16; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(16);
            fileChannel.read(byteBuffer);
            byteBuffer.flip();
            log.info("offset:{} position:{}", byteBuffer.getLong(), byteBuffer.getLong());
        }
    }

    @Test
    public void offsetIndexTest() throws FileNotFoundException {
        String filePath = storageProperties.getDataPath() + File.separator + topic + File.separator + 0 + File.separator + "00000000000000000000";
        OffsetIndex offsetIndex = new OffsetIndex(filePath);
        long[] queryOffsets = new long[]{9993, 9998};
        for (long offset : queryOffsets) {
            Result<IndexEntry.OffsetPosition> offsetPositionResult = offsetIndex.lookUp(offset);
            if (offsetPositionResult.failure()) {
                log.warn("find offset {} fail! ", offset);
            } else {
                IndexEntry.OffsetPosition offsetPosition = offsetPositionResult.getData();
                log.info("offset:{} position:{}", offsetPosition.getOffset(), offsetPosition.getPosition());
            }
        }
    }

    @Test
    public void loadTest() throws InterruptedException {
        Thread.sleep(70000);
    }

}
