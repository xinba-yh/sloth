package com.tsingj.sloth.broker;

import com.alibaba.fastjson.JSON;
import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.store.datajson.topic.TopicConfig;
import com.tsingj.sloth.store.datajson.topic.TopicManager;
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
        Result<TopicConfig> getTopicConfigResult = topicManager.getTopic("test-topic", true);
        System.out.println(JSON.toJSONString(getTopicConfigResult));
    }

    @Test
    public void getTest() {
        Result<TopicConfig> getTopicConfigResult = topicManager.getTopic("test-topic", false);
        System.out.println(JSON.toJSONString(getTopicConfigResult));
    }

    @Test
    public void autoAssignPartition() {
        Result<TopicConfig> getTopicConfigResult = topicManager.getTopic("test-topic", true);
        TopicConfig topicConfig = getTopicConfigResult.getData();
        for (int i = 0; i < 10; i++) {
            int assignPartition = topicManager.autoAssignPartition(topicConfig);
            log.info("time:{} assign partition:{}", i + 1, assignPartition);
        }


    }

}
