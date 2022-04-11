package com.tsingj.sloth.store.datajson;

import com.tsingj.sloth.store.datajson.offset.ConsumerGroupOffsetManager;
import com.tsingj.sloth.store.datajson.topic.TopicManager;
import com.tsingj.sloth.store.properties.StorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author yanghao
 */
@Component
public class DataJsonManager implements SchedulingConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(DataJsonManager.class);

    private final StorageProperties storageProperties;

    private final TopicManager topicManager;

    private final ConsumerGroupOffsetManager consumerGroupOffsetManager;


    public DataJsonManager(StorageProperties storageProperties, TopicManager topicManager, ConsumerGroupOffsetManager consumerGroupOffsetManager) {
        this.storageProperties = storageProperties;
        this.topicManager = topicManager;
        this.consumerGroupOffsetManager = consumerGroupOffsetManager;
    }


    @PostConstruct
    public void init() {

        topicManager.load();
        consumerGroupOffsetManager.load();

    }


    //----------------------------------------------------------scheduling--------------------------------------------------------------------

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        int consumerOffsetPersistenceInterval = storageProperties.getConsumerOffsetPersistenceInterval();
        logger.info("add consumerOffset persistence interval {}ms.", consumerOffsetPersistenceInterval);
        IntervalTask consumerOffsetPersistenceTask = new IntervalTask(this::consumerOffsetPersistence, consumerOffsetPersistenceInterval, consumerOffsetPersistenceInterval);
        taskRegistrar.addFixedDelayTask(consumerOffsetPersistenceTask);
    }

    private void consumerOffsetPersistence() {
        consumerGroupOffsetManager.persist();
    }


}
