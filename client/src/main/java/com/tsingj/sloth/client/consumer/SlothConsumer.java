package com.tsingj.sloth.client.consumer;

import com.tsingj.sloth.client.SlothClient;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import lombok.extern.slf4j.Slf4j;


/**
 * @author yanghao
 */
@Slf4j
public class SlothConsumer extends SlothClient {

    public SlothConsumer(SlothClientProperties clientProperties) {
        this.clientProperties = clientProperties;
        this.pollName = "sloth-producer";
    }

    public void start() {
        this.initConnect();
        log.info("sloth consumer init done.");
    }

    public void close() {
        this.closeConnect();
        log.info("sloth consumer destroy.");
    }


    //----------------------------------------------

}
