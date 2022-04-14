package com.tsingj.sloth.client;

import com.tsingj.sloth.client.springsupport.CommonConstants;
import com.tsingj.sloth.client.springsupport.RemoteProperties;
import com.tsingj.sloth.common.exception.ClientConnectException;
import com.tsingj.sloth.common.exception.ClientConnectTimeoutException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author yanghao
 */
@Slf4j
public class SlothRemoteClient {

    /**
     * The rpc client options.
     */
    private final RemoteProperties remoteProperties;

    /**
     * The worker group.
     */
    private EventLoopGroup workerGroup;

    /**
     * The Constant CLIENT_T_NAME.
     */
    private final static String POLL_NAME = "sloth";

    private final Bootstrap bootstrap = new Bootstrap();

    private volatile Channel channel;

    private String clientId;

    private final int retryInterval = 1000;

    private final Object lock = new Object();

    private volatile boolean stopped = false;


    public SlothRemoteClient(RemoteProperties remoteProperties) {
        this.remoteProperties = remoteProperties;
        this.initConnect();
    }

    public void initConnect() {
        if (this.remoteProperties.getIoEventGroupType() == CommonConstants.EventGroupMode.POLL_EVENT_GROUP) {
            this.workerGroup = new NioEventLoopGroup(this.remoteProperties.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(POLL_NAME));
        } else {
            this.workerGroup = new EpollEventLoopGroup(this.remoteProperties.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(POLL_NAME));
        }

        this.bootstrap.group(this.workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, this.remoteProperties.getReuseAddress())
                .option(ChannelOption.SO_KEEPALIVE, this.remoteProperties.getKeepAlive())
                .option(ChannelOption.TCP_NODELAY, this.remoteProperties.getTcpNoDelay())
                .option(ChannelOption.SO_SNDBUF, this.remoteProperties.getSndBufSize())
                .option(ChannelOption.SO_RCVBUF, this.remoteProperties.getRcvBufSize())
                .handler(new RemoteClientChannelInitializer(this.remoteProperties.getMaxSize()));
        try {
            String[] brokerUrlArr = this.remoteProperties.getBrokerUrl().split(":");
            ChannelFuture channelFuture = bootstrap.connect(brokerUrlArr[0], Integer.parseInt(brokerUrlArr[1]));
            this.channel = channelFuture.sync().channel();
            this.clientId = UUID.randomUUID().toString();

        } catch (Throwable e) {
            log.warn("Init sloth client fail!");
        }
    }

    public void closeConnect() {
        this.stopped = true;

        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }

        if (this.channel != null) {
            this.channel.close().syncUninterruptibly();
        }
    }

    public Channel getChannel() {
        if (!this.channel.isActive() && !this.stopped) {
            synchronized (lock) {
                if (!this.channel.isActive() && !this.stopped) {
                    log.info("thread:{} channel unActive! try reconnect!", Thread.currentThread().getId());
                    Integer connectTimeout = this.remoteProperties.getConnectTimeout();
                    String[] brokerUrlArr = this.remoteProperties.getBrokerUrl().split(":");
                    ChannelFuture channelFuture = this.bootstrap.connect(brokerUrlArr[0], Integer.parseInt(brokerUrlArr[1]));
                    //awaitUninterruptibly 底层为有锁。
                    if (channelFuture.awaitUninterruptibly(connectTimeout, TimeUnit.MILLISECONDS)) {
                        if (channelFuture.channel() != null && channelFuture.channel().isActive()) {
                            this.channel = channelFuture.channel();
                            log.info("sloth client channel reconnect success.");
                        } else {
                            throw new ClientConnectException("sloth client channel connect fail!");
                        }
                    } else {
                        throw new ClientConnectTimeoutException("sloth client channel connect timeout " + connectTimeout + " ms!");
                    }
                }
            }
        }
        return this.channel;
    }

    public String getClientId() {
        return this.clientId;
    }

}
