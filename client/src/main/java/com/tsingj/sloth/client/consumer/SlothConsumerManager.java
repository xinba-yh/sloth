package com.tsingj.sloth.client.consumer;

import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import lombok.extern.slf4j.Slf4j;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yanghao
 */
@Slf4j
public class SlothConsumerManager {

    private final SlothClientProperties slothClientProperties;

    public SlothConsumerManager(SlothClientProperties slothClientProperties) {
        this.slothClientProperties = slothClientProperties;
    }

    private final static Map<String, SlothConsumer> SLOTH_CONSUMER_MAP = new ConcurrentHashMap<>();

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
                SLOTH_CONSUMER_MAP.put(entry.getKey(), slothConsumer);
                log.info("init consumer {} success.", entry.getKey());
            } catch (Throwable e) {
                log.error("init consumer {} error!", entry.getKey(), e);
                throw new RuntimeException(e);
            }
        }
    }

    private void close() {
        for (SlothConsumer slothConsumer : SLOTH_CONSUMER_MAP.values()) {
            slothConsumer.close();
        }
    }

    public static SlothConsumer getSlothConsumer(String topic){
        return SLOTH_CONSUMER_MAP.get(topic);
    }

}

