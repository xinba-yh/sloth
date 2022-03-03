package com.tsingj.sloth.store;

import com.tsingj.sloth.store.log.*;
import com.tsingj.sloth.store.mock.ConsumerClient;
import com.tsingj.sloth.store.mock.ProducerClient;
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
import java.util.concurrent.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class StorageEngineTest {

    private static final Logger logger = LoggerFactory.getLogger(StorageEngineTest.class);

    @Autowired
    private StorageEngine storageEngine;

    @Autowired
    private StorageProperties storageProperties;

    private static final String topic = "test-topic";

    private static final int count = 100000;

    private static final int threadNum = 8;


    /**
     * 8 thread 10W -> 3S
     * 1 thread 80W -> 11S
     */
    @Test
    public void putMessageTest() {
        ExecutorService partitionPool = Executors.newFixedThreadPool(8);
        final ExecutorService partitionAppendPool = Executors.newFixedThreadPool(1);
        long startTime = System.currentTimeMillis();
        final CountDownLatch partitionCountDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            int finalI = i;
            partitionPool.execute(new Thread(() -> {
                //------------------test----------------------
                String helloWorld = "hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hellhello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello worldorld.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello worldorld.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello worldorld.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello worldorld.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello worldorld.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello worldorld.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello worldorld.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.hello world.";
                StopWatch sw = new StopWatch();
                Message message = new Message();
                message.setTopic(topic);
                message.setPartition(finalI);
                message.setBody(helloWorld.getBytes(StandardCharsets.UTF_8));
                sw.start();
                final CountDownLatch partitionAppendCountDownLatch = new CountDownLatch(count);
                for (int i1 = 0; i1 < count; i1++) {
                    int finalI1 = i1;
                    partitionAppendPool.execute(new Thread(() -> {
                        PutMessageResult putMessageResult = storageEngine.putMessage(message);
                        if (putMessageResult.getStatus() != PutMessageStatus.OK) {
                            logger.error("set error.{}", putMessageResult.getErrorMsg());
                        }
                        if (finalI1 == count - 1) {
                            logger.info("end offset:{}", putMessageResult.getOffset());
                        }
                        partitionAppendCountDownLatch.countDown();
                    }));
                }
                try {
                    partitionAppendCountDownLatch.await();
                } catch (InterruptedException ignored) {
                }
                sw.stop();
                System.out.println("put count:" + count + ",cost:" + sw.getTotalTimeMillis());
                partitionCountDownLatch.countDown();
            }));

        }

        try {
            partitionCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("put count:" + count * threadNum + ", all cost:" + (System.currentTimeMillis() - startTime) + "ms");

    }

    @Test
    public void getMessageTest() {
        for (int j = 0; j < 10; j++) {
            //random get message
            int loopCount = 10000;
            StopWatch sw = new StopWatch();
            int[] random = this.random(loopCount);
            sw.start();
            for (int i = 0; i < loopCount; i++) {
                long offset = (i == 0) ? 0L : random[i];
                GetMessageResult result = storageEngine.getMessage(topic, 0, offset);
                if (result.getStatus() != GetMessageStatus.FOUND) {
                    logger.warn("get msg fail,{}:{}! ", result.getStatus(), result.getErrorMsg());
                } else {
                    if (offset != result.getMessage().getOffset()) {
                        logger.warn("get msg fail，query:{}，got:{}! ", offset, result.getMessage().getOffset());
                    }
//                    log.info("get message offset:{} message:{}", offset, result.getMessage().getOffset());
                }
            }
            sw.stop();
            logger.info("data count {} , query {} times , cost:{}", count, loopCount, sw.getTotalTimeMillis());
        }
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
//        System.out.println(Arrays.toString(id));
        return id;
    }

    @Test
    public void offsetIndexReaderTest() throws IOException {
        File file = new File(storageProperties.getDataPath() + File.separator + topic + File.separator + 0 + File.separator + "00000000000000008389.index");
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

        for (int i = 0; i < file.length() / 16; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(16);
            fileChannel.read(byteBuffer);
            byteBuffer.flip();
            logger.info("offset:{} position:{}", byteBuffer.getLong(), byteBuffer.getLong());
        }
    }

    @Test
    public void offsetIndexTest() throws FileNotFoundException {
        String filePath = storageProperties.getDataPath() + File.separator + topic + File.separator + 0 + File.separator + "00000000000000000000";
        OffsetIndex offsetIndex = new OffsetIndex(filePath);
        long[] queryOffsets = new long[]{9993, 9998};
        for (long offset : queryOffsets) {
            Result<OffsetIndex.LogPositionSlotRange> logPositionRangeResult = offsetIndex.lookUp(offset);
            if (logPositionRangeResult.failure()) {
                logger.warn("find offset {} fail! ", offset);
            } else {
                OffsetIndex.LogPositionSlotRange logPositionRangeResultData = logPositionRangeResult.getData();
                logger.info("offset:{} position range:{} {}",offset, logPositionRangeResultData.getStart(), logPositionRangeResultData.getEnd());
            }
        }
    }

//    @Test
//    public void loadTest() throws InterruptedException {
//
//        LogSegment logSegment = logSegmentSet.getLatestLogSegmentFile(topic, 0);
//        List<ByteBuffer> messagesFromFirst = logSegment.getMessagesFromFirst(100);
//        if (messagesFromFirst == null || messagesFromFirst.size() == 0) {
//            logger.warn("load empty messages.");
//            return;
//        }
//        for (ByteBuffer msgByteBuffer : messagesFromFirst) {
//            logger.info("offset:{} storeSize:{}", msgByteBuffer.getLong(), msgByteBuffer.getInt());
//        }
//    }


    @Test
    public void skipListTest() throws InterruptedException {
        ConcurrentSkipListMap<Integer, Integer> skipListMap = new ConcurrentSkipListMap<>();
        int id[] = new int[]{93, 115, 144, 169, 302, 335, 386, 538, 607, 633, 697, 721, 785, 803, 881, 895, 917, 946, 987, 993};
        Arrays.sort(id);
        System.out.println(Arrays.toString(id));
        for (int j = 0; j < 20; j++) {
            skipListMap.put(id[j], id[j]);
        }

        System.out.println(skipListMap.floorKey(2));
        System.out.println(skipListMap.floorKey(93));
        System.out.println(skipListMap.higherKey(93));
        System.out.println(skipListMap.floorKey(100));
        System.out.println(skipListMap.floorKey(539));

    }


    //----------------------------------------标准测试---------------------------------------------

    /**
     * 8S 90W
     */
    @Test
    public void mockPutClientTest() {
        //定义每个partition发送message数量 -> 总数量为：(9 * count)
        int partitionMsgCount = 100000;
        int partitionCount = 9;
        /*
         *  1、producer put message mock
         */
        ProducerClient producerClient = new ProducerClient(topic, partitionMsgCount, partitionCount, storageEngine);
        producerClient.start();

    }


    /**
     * 5.5S -> 90W
     */
    @Test
    public void mockGetClientTest() {
        //定义每个partition发送message数量 -> 总数量为：(9 * count)
        int partitionMsgCount = 100000;
        int partitionCount = 9;

        //2、consumer get message mock

        ConsumerClient consumerClient = new ConsumerClient(topic, partitionMsgCount, partitionCount, storageEngine);
        consumerClient.start();

    }

}
