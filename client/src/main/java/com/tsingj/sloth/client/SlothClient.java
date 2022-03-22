package com.tsingj.sloth.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yanghao
 */
@Slf4j
public class SlothClient {

    /**
     * The rpc client options.
     */
    private SlothClientOptions slothClientOptions;

    /**
     * The worker group.
     */
    private EventLoopGroup workerGroup;

    /**
     * The Constant CLIENT_T_NAME.
     */
    private static final String CLIENT_THREAD_NAME = "sloth-client";

    /**
     * The correlation id.
     */
    private static final AtomicLong CORRELATION_ID = new AtomicLong(1);

    /**
     * correlation id -> responseMap
     */
    protected static final ConcurrentMap<Long /* correlationId */, ResponseFuture> CORRELATION_ID_RESPONSE_MAP = new ConcurrentHashMap<>(256);

    private Channel channel;

    public SlothClient(SlothClientOptions slothClientOptions) {
        this.slothClientOptions = slothClientOptions;
    }


    public void start() {
        if (this.slothClientOptions.getIoEventGroupType() == SlothClientOptions.POLL_EVENT_GROUP) {
            this.workerGroup = new NioEventLoopGroup(this.slothClientOptions.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(CLIENT_THREAD_NAME));
        } else {
            this.workerGroup = new EpollEventLoopGroup(this.slothClientOptions.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(CLIENT_THREAD_NAME));
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, slothClientOptions.isReuseAddress())
                .option(ChannelOption.SO_KEEPALIVE, slothClientOptions.isKeepAlive())
                .option(ChannelOption.TCP_NODELAY, slothClientOptions.isTcpNoDelay())
                .option(ChannelOption.SO_SNDBUF, slothClientOptions.getSndBufSize())
                .option(ChannelOption.SO_RCVBUF, slothClientOptions.getRcvBufSize())
                .handler(new RemoteClientChannelInitializer(slothClientOptions.getMaxSize()));
        try {
            this.channel = bootstrap.connect(this.slothClientOptions.getHost(), this.slothClientOptions.getPort()).sync().channel();
        } catch (InterruptedException e) {
            throw new RuntimeException("Init producer client fail!", e);
        }
        log.info("sloth client init done.");
    }

    public void close() {

        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }

        if (this.channel != null) {
            try {
                this.channel.close().await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("close channel fail!");
            }
        }

    }


    //----------------------------------------------

    public void sendOneway(Remoting.Message message) {
        DataPackage dataPackage = DataPackage.builder()
                .magicCode(ProtocolConstants.MAGIC_CODE)
                .version(ProtocolConstants.VERSION)
                .command(ProtocolConstants.Command.SEND_MESSAGE)
                .requestType(ProtocolConstants.RequestType.ONE_WAY)
                .timestamp(System.currentTimeMillis())
                .data(message.toByteArray())
                .build();
        this.channel.writeAndFlush(dataPackage);
    }

    public Remoting.SendResult send(Remoting.Message message) {
        long currentCorrelationId = CORRELATION_ID.getAndAdd(1);
        ResponseFuture responseFuture = new ResponseFuture(currentCorrelationId);
        //add 关联关系，handler或者超时的定时任务将会清理。
        CORRELATION_ID_RESPONSE_MAP.put(currentCorrelationId, responseFuture);
        try {
            DataPackage dataPackage = DataPackage.builder()
                    .magicCode(ProtocolConstants.MAGIC_CODE)
                    .version(ProtocolConstants.VERSION)
                    .command(ProtocolConstants.Command.SEND_MESSAGE)
                    .requestType(ProtocolConstants.RequestType.SYNC)
                    .correlationId(currentCorrelationId)
                    .timestamp(System.currentTimeMillis())
                    .data(message.toByteArray())
                    .build();

            //send data
            this.channel.writeAndFlush(dataPackage);

            DataPackage responseData = responseFuture.waitResponse(slothClientOptions.getOnceTalkTimeout());
            if (responseData == null) {
                log.warn("correlationId {} wait response null!", currentCorrelationId);
                return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("receive data null!").build();
            }
            byte[] data = responseData.getData();
            return Remoting.SendResult.parseFrom(data);
        } catch (InterruptedException e) {
            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.TIMEOUT).build();
        } catch (InvalidProtocolBufferException e) {
            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("protobuf parse error!" + e.getMessage()).build();
        } finally {
            CORRELATION_ID_RESPONSE_MAP.remove(currentCorrelationId);
        }
    }

}
