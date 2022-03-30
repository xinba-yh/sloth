package com.tsingj.sloth.client.consumer;

import com.tsingj.sloth.client.SlothRemoteClient;
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

    private final SlothRemoteClient slothRemoteClient;

    private final SlothClientProperties slothClientProperties;

    public SlothConsumerManager(SlothClientProperties slothClientProperties, SlothRemoteClient slothRemoteClient) {
        this.slothClientProperties = slothClientProperties;
        this.slothRemoteClient = slothRemoteClient;
    }

    private final static Map<String, SlothRemoteConsumer> SLOTH_CONSUMER_MAP = new ConcurrentHashMap<>();

    private void init() {
        Map<String, ConsumerProperties> consumerMap = slothClientProperties.getConsumer();
        if (consumerMap == null || consumerMap.size() == 0) {
            return;
        }
        for (Map.Entry<String, ConsumerProperties> entry : consumerMap.entrySet()) {
            log.info("prepare init consumer {}.", entry.getKey());
            try {
                SlothRemoteConsumer slothConsumer = new SlothRemoteConsumer(slothClientProperties, entry.getValue(), slothRemoteClient);
                slothConsumer.init();
                SLOTH_CONSUMER_MAP.put(entry.getKey(), slothConsumer);
                log.info("init consumer {} success.", entry.getKey());
            } catch (Throwable e) {
                log.error("init consumer {} error!", entry.getKey(), e);
                throw new RuntimeException(e);
            }
        }
    }

    private void destroy() {
        for (SlothRemoteConsumer slothConsumer : SLOTH_CONSUMER_MAP.values()) {
            slothConsumer.destroy();
        }
    }

    public static SlothRemoteConsumer getSlothConsumer(String topic) {
        return SLOTH_CONSUMER_MAP.get(topic);
    }

}

