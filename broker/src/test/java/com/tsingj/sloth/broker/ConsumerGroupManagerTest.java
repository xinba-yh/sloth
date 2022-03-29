package com.tsingj.sloth.broker;

import com.alibaba.fastjson.JSON;
import com.tsingj.sloth.broker.service.ConsumerGroupManager;
import com.tsingj.sloth.store.datajson.topic.TopicConfig;
import com.tsingj.sloth.store.datajson.topic.TopicManager;
import com.tsingj.sloth.store.pojo.Result;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class ConsumerGroupManagerTest {

    @Autowired
    private ConsumerGroupManager consumerGroupManager;


    @Test
    public void heartBeatTest() {
        for (int i = 0; i < 9; i++) {
            String clientId = "test-client" + (i + 1);
            String groupName = "test-group";
            String topic = "test-topic";
            Channel channel = null;
            consumerGroupManager.heartbeat(clientId, groupName, topic, channel);
            ConcurrentHashMap<String, ConsumerGroupManager.ConsumerChannel> consumerChannelMap = consumerGroupManager.getConsumerChannelMap(groupName, topic);
            System.out.println("consumerInfo:" + JSON.toJSONString(consumerChannelMap, true));
        }
    }

}
