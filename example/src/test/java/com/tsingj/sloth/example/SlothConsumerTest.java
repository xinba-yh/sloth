package com.tsingj.sloth.example;

import com.tsingj.sloth.example.listener.MessageOrderedListener;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
@ActiveProfiles("consumer")
@RunWith(SpringJUnit4ClassRunner.class)
public class SlothConsumerTest {

    /**
     * 测试消费数量
     */
    @Value("${test.consumer.count}")
    private int testConsumerCount;

    private final String topic = "test-topic";

    @Autowired
    private MessageOrderedListener messageOrderedListener;

    /**
     * 1S 10W server收到
     */
    @Test
    public void consumerTest() throws InterruptedException {
        log.info("prepare test consumer topic:{} count:{}.", topic, testConsumerCount);
        long startTime = System.currentTimeMillis();
        messageOrderedListener.getCountDownLatch().await(120, TimeUnit.SECONDS);
        log.info("test consumer topic:{} count:{} done, cost:{}", topic, testConsumerCount, System.currentTimeMillis() - startTime);
        ConcurrentHashMap<Integer, CopyOnWriteArrayList<Long>> collectors = messageOrderedListener.getCollector();
        for (ConcurrentHashMap.Entry<Integer, CopyOnWriteArrayList<Long>> entry : collectors.entrySet()) {
            int partition = entry.getKey();
            CopyOnWriteArrayList<Long> consumeOffsets = entry.getValue();
            Set<Long> distinctOffsets = new HashSet<>(consumeOffsets);
            log.info("partition:{} consume offsets size:{} distinct:{}, values:{}", partition, consumeOffsets.size(), distinctOffsets.size(), consumeOffsets);
        }
    }

}
