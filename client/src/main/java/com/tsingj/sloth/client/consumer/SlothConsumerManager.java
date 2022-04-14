package com.tsingj.sloth.client.consumer;

import lombok.extern.slf4j.Slf4j;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yanghao
 */
@Slf4j
public class SlothConsumerManager {


    private final static Map<String, SlothRemoteConsumer> SLOTH_CONSUMER_MAP = new ConcurrentHashMap<>();


    public static void register(SlothRemoteConsumer slothRemoteConsumer) {
        SLOTH_CONSUMER_MAP.put(slothRemoteConsumer.getTopic(), slothRemoteConsumer);
    }

    public static void unregister(String topic) {
        SLOTH_CONSUMER_MAP.remove(topic);
    }

    public static SlothRemoteConsumer get(String topic) {
        return SLOTH_CONSUMER_MAP.get(topic);
    }

}

