package com.tsingj.sloth.client;

import com.tsingj.sloth.client.springsupport.CommonConstants;
import com.tsingj.sloth.client.springsupport.ConnectProperties;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * @author yanghao
 */
@Slf4j
public class SlothRemoteClient {

    /**
     * The rpc client options.
     */
    protected SlothClientProperties clientProperties;

    /**
     * The worker group.
     */
    protected EventLoopGroup workerGroup;

    /**
     * The Constant CLIENT_T_NAME.
     */
    protected String pollName = "sloth";


    protected Channel channel;

    private String clientId;


    public SlothRemoteClient(SlothClientProperties clientProperties) {
        this.clientProperties = clientProperties;
    }

    public void initConnect() {
        ConnectProperties connectProperties = clientProperties.getConnect();
        if (connectProperties.getIoEventGroupType() == CommonConstants.EventGroupMode.POLL_EVENT_GROUP) {
            this.workerGroup = new NioEventLoopGroup(connectProperties.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(this.pollName));
        } else {
            this.workerGroup = new EpollEventLoopGroup(connectProperties.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(this.pollName));
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, connectProperties.getReuseAddress())
                .option(ChannelOption.SO_KEEPALIVE, connectProperties.getKeepAlive())
                .option(ChannelOption.TCP_NODELAY, connectProperties.getTcpNoDelay())
                .option(ChannelOption.SO_SNDBUF, connectProperties.getSndBufSize())
                .option(ChannelOption.SO_RCVBUF, connectProperties.getRcvBufSize())
                .handler(new RemoteClientChannelInitializer(connectProperties.getMaxSize()));
        try {
            String[] brokerUrlArr = clientProperties.getBrokerUrl().split(":");
            this.channel = bootstrap.connect(brokerUrlArr[0], Integer.parseInt(brokerUrlArr[1])).sync().channel();
            this.clientId = UUID.randomUUID().toString();
        } catch (InterruptedException e) {
            throw new RuntimeException("Init producer client fail!", e);
        }
    }

    public void closeConnect() {
        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }

        if (this.channel != null) {
            this.channel.close().syncUninterruptibly();
        }
    }

    public Channel getChannel() {
        if (!this.channel.isActive()) {
            log.warn("channel unActive! try reconnect!");
            //重连
            this.closeConnect();
            this.initConnect();
        }
        return this.channel;
    }

    public String getClientId(){
        return this.clientId;
    }

}
