package com.tsingj.sloth.broker;

import com.tsingj.sloth.broker.handler.BrokerServerChannelInitializer;
import com.tsingj.sloth.broker.properties.BrokerProperties;
import com.tsingj.sloth.store.properties.StorageProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class BrokerServer {

    private final BrokerProperties brokerProperties;

    private final StorageProperties storageProperties;

    public BrokerServer(BrokerProperties brokerProperties, StorageProperties storageProperties) {
        this.brokerProperties = brokerProperties;
        this.storageProperties = storageProperties;
    }

    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workGroup = new NioEventLoopGroup();

    public void start() throws Exception {

        try {
            ServerBootstrap b = new ServerBootstrap()
                    .group(bossGroup, workGroup)
                    //todo add epoll condition
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new BrokerServerChannelInitializer(storageProperties.getMessageMaxSize()))
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);
            Channel ch = b.bind(brokerProperties.getPort()).sync().channel();
            log.info("broker server start on port:" + brokerProperties.getPort());
            ch.closeFuture().sync();
        } finally {
            close();
        }
    }

    @PreDestroy
    public void close() {
        bossGroup.shutdownGracefully();
        workGroup.shutdownGracefully();
        log.error("broker server stopped!");
    }


}
