package com.tsingj.sloth.broker;


import com.tsingj.sloth.broker.constants.EventType;
import com.tsingj.sloth.broker.event.AsyncEvent;
import com.tsingj.sloth.common.SpringContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class EventTest {

    @Test
    public void eventTest() throws InterruptedException {
        AsyncEvent asyncEvent = new AsyncEvent(this, EventType.CHANNEL_CLOSED, "1111");
        SpringContextHolder.publishEvent(asyncEvent);
        Thread.sleep(100);
    }

}
