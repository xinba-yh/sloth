package com.tsingj.sloth.broker.configuration;

import com.tsingj.sloth.broker.service.ConsumerGroupOffsetManager;
import com.tsingj.sloth.broker.service.TopicManager;
import com.tsingj.sloth.common.threadpool.fixed.FixedThreadPool;
import com.tsingj.sloth.common.threadpool.fixed.FixedThreadPoolExecutor;
import com.tsingj.sloth.store.properties.StorageProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import javax.annotation.PostConstruct;

/**
 * @author yanghao
 */

@Slf4j
@EnableScheduling
@EnableAsync
@Configuration
public class BrokerConfiguration implements SchedulingConfigurer {

    @Autowired
    private TopicManager topicManager;

    @Autowired
    private ConsumerGroupOffsetManager consumerGroupOffsetManager;

    @Autowired
    private StorageProperties storageProperties;

    @Bean
    public FixedThreadPoolExecutor asyncExecutor() {
        int cpuSize = Runtime.getRuntime().availableProcessors();
        FixedThreadPool fixedThreadPool = new FixedThreadPool();
        return (FixedThreadPoolExecutor) fixedThreadPool.getExecutor(cpuSize, cpuSize, Integer.MAX_VALUE, 30, "async_event");
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
        log.info("add consumerOffset persistence interval {}ms.", consumerOffsetPersistenceInterval);
        IntervalTask consumerOffsetPersistenceTask = new IntervalTask(this::consumerOffsetPersistence, consumerOffsetPersistenceInterval, consumerOffsetPersistenceInterval);
        taskRegistrar.addFixedDelayTask(consumerOffsetPersistenceTask);
    }

    private void consumerOffsetPersistence() {
        consumerGroupOffsetManager.persist();
    }


}
