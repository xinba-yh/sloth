package com.tsingj.sloth.client.consumer;

import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author yanghao
 */
@Slf4j
public class SlothConsumerManager {

    private final SlothClientProperties slothClientProperties;

    public SlothConsumerManager(SlothClientProperties slothClientProperties) {
        this.slothClientProperties = slothClientProperties;
    }

    private final static List<SlothConsumer> SLOTH_CONSUMERS = new ArrayList<>();

    private void start() {
        Map<String, ConsumerProperties> consumerMap = slothClientProperties.getConsumer();
        if (consumerMap == null || consumerMap.size() == 0) {
            return;
        }
        for (Map.Entry<String, ConsumerProperties> entry : consumerMap.entrySet()) {
            log.info("prepare init consumer {}.", entry.getKey());
            try {
                SlothConsumer slothConsumer = new SlothConsumer(slothClientProperties, entry.getValue());
                slothConsumer.start();
                SLOTH_CONSUMERS.add(slothConsumer);
                log.info("init consumer {} success.", entry.getKey());
            } catch (Throwable e) {
                log.error("init consumer {} error!", entry.getKey(), e);
                throw new RuntimeException(e);
            }
        }
    }

    private void close() {
        for (SlothConsumer slothConsumer : SLOTH_CONSUMERS) {
            log.info("prepare destroy {} consumer.", slothConsumer.getTopic());
            slothConsumer.close();
        }
    }


}

