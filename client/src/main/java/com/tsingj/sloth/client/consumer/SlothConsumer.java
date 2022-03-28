package com.tsingj.sloth.client.consumer;

import com.tsingj.sloth.client.SlothClient;
import com.tsingj.sloth.client.springsupport.ConsumerProperties;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import lombok.extern.slf4j.Slf4j;


/**
 * @author yanghao
 */
@Slf4j
public class SlothConsumer extends SlothClient {

    private final ConsumerProperties consumerProperties;

    public SlothConsumer(SlothClientProperties clientProperties, ConsumerProperties consumerProperties) {
        this.clientProperties = clientProperties;
        this.consumerProperties = consumerProperties;
        this.pollName = "sloth-producer";
    }

    public void start() {
        this.initConnect();
        this.heartbeat();
        log.info("sloth consumer init done.");
    }

    private void heartbeat() {
        //1、立即发送一次heartbeat，获取消费者应该消费的

    }

    public void close() {
        this.closeConnect();
        log.info("sloth consumer destroy.");
    }

    public String getTopic() {
        return consumerProperties.getTopic();
    }


    //----------------------------------------------

}
