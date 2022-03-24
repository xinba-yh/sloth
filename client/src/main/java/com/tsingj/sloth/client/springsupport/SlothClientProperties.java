package com.tsingj.sloth.client.springsupport;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author yanghao
 */
@Data
@ConfigurationProperties(prefix = "spring.sloth")
public class SlothClientProperties {

    private String brokerUrl;

    private ProducerProperties producer;

    private ConsumerProperties consumer;

    private ConnectProperties connect;

}
