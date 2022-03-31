package com.tsingj.sloth.example;

import com.tsingj.sloth.client.consumer.MessageListener;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import com.tsingj.sloth.remoting.message.Remoting;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@Slf4j
@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
public class SlothConsumerTest {

    @Autowired
    private SlothClientProperties slothClientProperties;

    /**
     * 1S 10W server收到
     */
    @Test
    public void consumerTest() throws InterruptedException {
        Thread.sleep(5000);
    }

    @Test
    public void newListener() throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        String listenerClass = slothClientProperties.getConsumer().get("test-topic").getListener();
        Class<MessageListener> messageListenerClass = (Class<MessageListener>) Class.forName(listenerClass);
        MessageListener messageListener = messageListenerClass.newInstance();

        messageListener.consumeMessage(Remoting.GetMessageResult.Message.newBuilder().setTopic("123").build());


    }

}
