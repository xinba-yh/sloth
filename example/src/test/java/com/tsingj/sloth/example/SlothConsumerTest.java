package com.tsingj.sloth.example;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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
