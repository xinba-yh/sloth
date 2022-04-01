package com.tsingj.sloth.broker.configuration;

import com.tsingj.sloth.common.threadpool.fixed.FixedThreadPool;
import com.tsingj.sloth.common.threadpool.fixed.FixedThreadPoolExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * @author yanghao
 */
@EnableAsync
@Configuration
public class AsyncConfiguration {

    @Bean
    public FixedThreadPoolExecutor asyncExecutor() {
        int cpuSize = Runtime.getRuntime().availableProcessors();
        FixedThreadPool fixedThreadPool = new FixedThreadPool();
        return (FixedThreadPoolExecutor) fixedThreadPool.getExecutor(cpuSize, cpuSize, Integer.MAX_VALUE, 30, "async_event");
    }

}
