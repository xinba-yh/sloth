package com.tsingj.sloth.store.datajson;

import com.tsingj.sloth.store.datajson.topic.TopicManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class DataJsonManager {

    @Autowired
    private TopicManager topicManager;


    @PostConstruct
    public void init() {
        topicManager.load();
    }

    // TODO: 2022/3/25 add scheduler cache persistence

}
