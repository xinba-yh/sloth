package com.tsingj.sloth.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.PackageCodec;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yanghao
 */
public class ProducerClient {

    /**
     * The rpc client options.
     */
    private ClientOptions clientOptions;

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

    public ProducerClient(ClientOptions clientOptions) {
        this.clientOptions = clientOptions;
    }


    public void start() {
        if (this.clientOptions.getIoEventGroupType() == ClientOptions.POLL_EVENT_GROUP) {
            this.workerGroup = new NioEventLoopGroup(this.clientOptions.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(CLIENT_THREAD_NAME));
        } else {
            this.workerGroup = new EpollEventLoopGroup(this.clientOptions.getWorkGroupThreadSize(),
                    new DefaultThreadFactory(CLIENT_THREAD_NAME));
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.workerGroup).channel(NioSocketChannel.class)
                .handler(new RemoteClientChannelInitializer(clientOptions.getMaxSize()))
                .option(ChannelOption.SO_REUSEADDR, clientOptions.isReuseAddress())
                .option(ChannelOption.SO_KEEPALIVE, clientOptions.isKeepAlive())
                .option(ChannelOption.TCP_NODELAY, clientOptions.isTcpNoDelay());
        try {
            this.channel = bootstrap.connect(this.clientOptions.getHost(), this.clientOptions.getPort()).sync().channel();
        } catch (InterruptedException e) {
            throw new RuntimeException("Init producer client fail!", e);
        }
    }

    public void close() {

        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }

        if (this.channel != null) {
            try {
                this.channel.close().sync();
            } catch (InterruptedException ignored) {
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

            DataPackage responseData = responseFuture.waitResponse(clientOptions.getOnceTalkTimeout());
            if (responseData == null) {
                return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("receive data null!").build();
            }
            byte[] data = responseData.getData();
            return Remoting.SendResult.parseFrom(data);
        } catch (InterruptedException e) {
            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.TIMEOUT).build();
        } catch (InvalidProtocolBufferException e) {
            return Remoting.SendResult.newBuilder().setRetCode(Remoting.SendResult.RetCode.ERROR).setErrorInfo("protobuf parse error!" + e.getMessage()).build();
        }
    }

}
