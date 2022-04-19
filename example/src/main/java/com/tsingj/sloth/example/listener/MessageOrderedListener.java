package com.tsingj.sloth.example.listener;

import com.tsingj.sloth.client.consumer.ConsumeStatus;
import com.tsingj.sloth.client.consumer.MessageListener;
import com.tsingj.sloth.remoting.message.Remoting;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;


/**
 * @author yanghao
 * one partition one thread.
 */

@Slf4j
@Component
public class MessageOrderedListener implements MessageListener {

    private CountDownLatch countDownLatch;

    private ConcurrentHashMap<Integer, CopyOnWriteArrayList<Long>> collector = new ConcurrentHashMap<>();

    @Override
    public ConsumeStatus consumeMessage(Remoting.GetMessageResult.Message msg) {
        CopyOnWriteArrayList<Long> offsetList = collector.computeIfAbsent(msg.getPartition(), s -> new CopyOnWriteArrayList<>());
        offsetList.add(msg.getOffset());
        countDownLatch.countDown();
        if (msg.getOffset() % 100 == 0) {
            log.info("Thread:{} topic:{} partition:{} offset:{}", Thread.currentThread().getId(), msg.getTopic(), msg.getPartition(), msg.getOffset());
        }
        return ConsumeStatus.SUCCESS;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public ConcurrentHashMap<Integer, CopyOnWriteArrayList<Long>> getCollector() {
        return collector;
    }

}
