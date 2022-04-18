package com.tsingj.sloth.broker.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author yanghao
 */
@ConfigurationProperties(prefix = "spring.sloth.broker")
@Component
public class BrokerProperties {

    private int port;

    /**
     * 丢失消费者心跳的最大时间，S为单位。
     */
    private int loseConsumerHbMaxMills = 9;

    private int backLogSize = 1024;

    /**
     * The tcp no delay.
     */
    private boolean tcpNoDelay = true;

    /**
     * The reuse address.
     */
    private boolean reuseAddress = true;

    /**
     * 发送缓冲区大小
     */
    private int sndBufSize = 65535;

    /**
     * 接收缓冲区大小
     */
    private int rcvBufSize = 65535;


    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getBackLogSize() {
        return backLogSize;
    }

    public void setBackLogSize(int backLogSize) {
        this.backLogSize = backLogSize;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public boolean isReuseAddress() {
        return reuseAddress;
    }

    public void setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
    }

    public int getSndBufSize() {
        return sndBufSize;
    }

    public void setSndBufSize(int sndBufSize) {
        this.sndBufSize = sndBufSize;
    }

    public int getRcvBufSize() {
        return rcvBufSize;
    }

    public void setRcvBufSize(int rcvBufSize) {
        this.rcvBufSize = rcvBufSize;
    }

    public int getLoseConsumerHbMaxMills() {
        return loseConsumerHbMaxMills;
    }

    public void setLoseConsumerHbMaxMills(int loseConsumerHbMaxMills) {
        this.loseConsumerHbMaxMills = loseConsumerHbMaxMills;
    }
}
