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

    private Map<String, ConsumerProperties> consumers;

    private RemoteProperties remote;

}
