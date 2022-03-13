package com.tsingj.sloth.broker.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author yanghao
 */
@ConfigurationProperties(prefix = "broker")
@Component
public class BrokerProperties {

    private int port;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
