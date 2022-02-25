package com.tsingj.sloth.store;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.charset.StandardCharsets;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class StorageEngineTest {

    @Autowired
    private StorageEngine storageEngine;

    @Test
    public void putMessageTest() {
        String helloWorld = "hello world.";
        Message message = Message.builder().topic("test-topic").partition(0).body(helloWorld.getBytes(StandardCharsets.UTF_8)).build();
        PutMessageResult putMessageResult = storageEngine.putMessage(message);
        System.out.println(JSON.toJSONString(putMessageResult));
    }

}
