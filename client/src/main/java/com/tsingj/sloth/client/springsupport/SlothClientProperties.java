package com.tsingj.sloth.client.springsupport;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import static com.tsingj.sloth.client.SlothClientOptions.POLL_EVENT_GROUP;

/**
 * @author yanghao
 */
@Data
@ConfigurationProperties(prefix = "spring.sloth")
public class SlothClientProperties {

    private String brokerUrl;

    /**
     * The connect timeout.
     */
    private Integer connectTimeout = 2000;

    /**
     * The once talk timeout.
     */
    private int onceTalkTimeout = 1000;

    /**
     * The work group thread size. Netty work thread size, default is NettyRuntime.availableProcessors() * 2
     */
    private int workGroupThreadSize = 0;

    /**
     * The max byte size to send and receive from buffer.
     */
    private int maxSize = 1024 * 1024 * 4;

    /**
     * The io event group type.
     */
    private int ioEventGroupType = POLL_EVENT_GROUP; // 0=poll, 1=epoll

}
