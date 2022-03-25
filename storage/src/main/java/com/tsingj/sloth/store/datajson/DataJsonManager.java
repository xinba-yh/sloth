package com.tsingj.sloth.store.datajson;

import com.tsingj.sloth.store.datajson.offset.ConsumerOffsetManager;
import com.tsingj.sloth.store.datajson.topic.TopicManager;
import com.tsingj.sloth.store.properties.StorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class DataJsonManager implements SchedulingConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(DataJsonManager.class);

    @Autowired
    private StorageProperties storageProperties;

    @Autowired
    private TopicManager topicManager;

    @Autowired
    private ConsumerOffsetManager consumerOffsetManager;


    @PostConstruct
    public void init() {

        topicManager.load();
        consumerOffsetManager.load();

    }


    //----------------------------------------------------------scheduling--------------------------------------------------------------------

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        logger.info("add consumerOffset persistence interval {}ms.", storageProperties.getConsumerOffsetPersistenceInterval());
        IntervalTask consumerOffsetPersistenceTask = new IntervalTask(this::consumerOffsetPersistence, storageProperties.getConsumerOffsetPersistenceInterval(), 10);
        taskRegistrar.addFixedDelayTask(consumerOffsetPersistenceTask);
    }

    private void consumerOffsetPersistence() {
        consumerOffsetManager.persist();
    }

}
