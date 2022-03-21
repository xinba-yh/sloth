package com.tsingj.sloth.client;

import org.springframework.util.Assert;

/**
 * @author yanghao
 */

public class SlothClientOptions {

    public SlothClientOptions(String brokerUrl) {
        String[] brokerUrlArr = brokerUrl.split(":");
        Assert.isTrue(brokerUrlArr.length == 2, "please check your brokerUrl! not expect [host:port] !¬");
        this.host = brokerUrlArr[0];
        this.port = Integer.parseInt(brokerUrlArr[1]);
        this.connectTimeout = 1000;
        this.keepAlive = true;
        this.tcpNoDelay = true;
        this.reuseAddress = true;
    }

    /**
     * broker host
     */
    private String host;

    /**
     * broker port
     */
    private int port;

    /**
     * The connect timeout.
     */
    private int connectTimeout;

    /**
     * The tcp no delay.
     */
    private boolean tcpNoDelay;

    /**
     * The keep alive.
     */
    private boolean keepAlive;

    /**
     * The reuse address.
     */
    private boolean reuseAddress;

    /**
     * The work group thread size. Netty work thread size, default is NettyRuntime.availableProcessors() * 2
     */
    private int workGroupThreadSize = 0;

    /**
     * The once talk timeout.
     */
    private int onceTalkTimeout = 1000;

    /**
     * The max byte size to send and receive from buffer.
     */
    private int maxSize = 1024 * 1024 * 4;

    /**
     * The Constant POLL_EVENT_GROUP.
     */
    public static final int POLL_EVENT_GROUP = 0;

    /**
     * The Constant EPOLL_EVENT_GROUP.
     */
    public static final int EPOLL_EVENT_GROUP = 1;

    /**
     * The io event group type.
     */
    private int ioEventGroupType = POLL_EVENT_GROUP; // 0=poll, 1=epoll


    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean isReuseAddress() {
        return reuseAddress;
    }

    public void setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
    }

    public int getWorkGroupThreadSize() {
        return workGroupThreadSize;
    }

    public void setWorkGroupThreadSize(int workGroupThreadSize) {
        this.workGroupThreadSize = workGroupThreadSize;
    }

    public int getOnceTalkTimeout() {
        return onceTalkTimeout;
    }

    public void setOnceTalkTimeout(int onceTalkTimeout) {
        this.onceTalkTimeout = onceTalkTimeout;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getIoEventGroupType() {
        return ioEventGroupType;
    }

    public void setIoEventGroupType(int ioEventGroupType) {
        this.ioEventGroupType = ioEventGroupType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
