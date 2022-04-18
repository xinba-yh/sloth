package com.tsingj.sloth.client.springsupport;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * @author yanghao
 */
@Data
@ConfigurationProperties(prefix = "spring.sloth")
public class SlothClientProperties {

    private RemoteProperties remote;

    private ProducerProperties producer;

    private Consumer consumer;

}
