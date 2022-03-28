package com.tsingj.sloth.client.springsupport;

import lombok.Data;

import static com.tsingj.sloth.client.springsupport.CommonConstants.EventGroupMode.POLL_EVENT_GROUP;

/**
 * @author yanghao
 */
@Data
public class ConnectProperties {

    /**
     * The connect timeout.
     */
    private Integer connectTimeout = 2000;

    /**
     * The once talk timeout.
     */
    private Integer onceTalkTimeout = 1000;

    /**
     * The work group thread size. Netty work thread size, default is NettyRuntime.availableProcessors() * 2
     */
    private Integer workGroupThreadSize = 0;

    /**
     * The io event group type.
     */
    private Integer ioEventGroupType = POLL_EVENT_GROUP; // 0=poll, 1=epoll


    /**
     * The reuse address.
     */
    private Boolean reuseAddress = true;

    /**
     * The tcp no delay.
     */
    private Boolean tcpNoDelay = true;

    /**
     * The keep alive.
     */
    private Boolean keepAlive = true;

    private Integer sndBufSize = 65535;

    private Integer rcvBufSize = 65535;

    /**
     * The max byte size to send and receive from buffer.
     */
    private Integer maxSize = 1024 * 1024 * 4;

}
