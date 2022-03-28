package com.tsingj.sloth.example;

import com.google.protobuf.ByteString;
import com.tsingj.sloth.client.producer.SlothProducer;
import com.tsingj.sloth.remoting.message.Remoting;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
public class SlothConsumerTest {


    /**
     * 1S 10W server收到
     */
    @Test
    public void consumerTest() throws InterruptedException {
        Thread.sleep(5000);
    }


}
