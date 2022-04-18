package com.tsingj.sloth.broker;

import com.alibaba.fastjson.JSON;
import com.tsingj.sloth.broker.service.TopicManager;

import com.tsingj.sloth.common.result.Result;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class TopicManagerTest {

    @Autowired
    private TopicManager topicManager;


    @Test
    public void getOrCreateTopicTest() {
        Result<TopicManager.TopicConfig> getTopicConfigResult = topicManager.getTopic("test-topic", true);
        System.out.println(JSON.toJSONString(getTopicConfigResult));
    }

    @Test
    public void getTest() {
        Result<TopicManager.TopicConfig> getTopicConfigResult = topicManager.getTopic("test-topic", false);
        System.out.println(JSON.toJSONString(getTopicConfigResult));
    }

    @Test
    public void autoAssignPartition() {
        Result<TopicManager.TopicConfig> getTopicConfigResult = topicManager.getTopic("test-topic", true);
        TopicManager.TopicConfig topicConfig = getTopicConfigResult.getData();
        for (int i = 0; i < 10; i++) {
            int assignPartition = topicManager.autoAssignPartition(topicConfig);
            log.info("time:{} assign partition:{}", i + 1, assignPartition);
        }
    }

}
