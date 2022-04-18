package com.tsingj.sloth.broker;

import com.tsingj.sloth.broker.handler.LiftCycleHandler;
import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.remoting.protocol.RemoteCommand;
import com.tsingj.sloth.remoting.protocol.PackageCodec;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class MyClient {

    public static void main(String[] args) throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class)
                    .handler(new MyClientInitializer());

            ChannelFuture channelFuture = bootstrap.connect("localhost", 9000).sync();
            channelFuture.channel().closeFuture().sync();
        } finally {
            eventLoopGroup.shutdownGracefully();
        }
    }


    public static class MyClientInitializer extends ChannelInitializer<SocketChannel> {

        private static final String SPLIT = "split";

        private static final String ENCODER = "encoder";

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("channel_state", new IdleStateHandler(0, 0, 90, TimeUnit.SECONDS));
            pipeline.addLast("channel_life_cycle", LiftCycleHandler.INSTANCE);

            //TCP拆包粘包
            //基本原理：不断从 TCP 缓冲区中读取数据，每次读取完都需要判断是否是一个完整的数据包
            //1、如果当前读取的数据不足以拼接成一个完整的业务数据包，那就保留该数据，继续从 TCP 缓冲区中读取，直到得到一个完整的数据包。
            //2、如果当前读到的数据加上已经读取的数据足够拼接成一个数据包，那就将已经读取的数据拼接上本次读取的数据，构成一个完整的业务数据包传递到业务逻辑，多余的数据仍然保留，以便和下次读到的数据尝试拼接。
            //netty提供的拆包粘包实现
            //1. 固定长度的拆包器 FixedLengthFrameDecoder
            //2. 行拆包器 LineBasedFrameDecoder
            //3. 分隔符拆包器 DelimiterBasedFrameDecoder
            //4. 基于长度域拆包器 LengthFieldBasedFrameDecoder
            /*
             *   1、自定义协议如下：
             *      magic_code 5字节
             *      correlation_id 8字节
             *      version  1字节
             *      command  1字节
             *      dataLen  4字节
             *      data     N字节
             *   2、计算LengthFieldBasedFrameDecoder入参
             *      maxFrameLength = storage.maxMessageSize
             *      长度字段的offset -> lengthFieldOffset = magic_code + correlationId + version + command = 15
             *      长度字段大小 -> lengthFieldLength = dataLen = 4
             */
            int maxMessageSize = 1024 * 1024 * 4;
            pipeline.addLast(SPLIT, new LengthFieldBasedFrameDecoder(maxMessageSize, ProtocolConstants.FieldLength.MAGIC_CODE + ProtocolConstants.FieldLength.VERSION + ProtocolConstants.FieldLength.COMMAND, ProtocolConstants.FieldLength.DATA_LEN));
//            pipeline.addLast(ENCODER, new PackageEncodeHandler());
            pipeline.addLast(new MyClientHandler());
        }
    }


    public static class MyClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
            //服务端的远程地址
            System.out.println(ctx.channel().remoteAddress());

        }

        /**
         * 当服务器端与客户端进行建立连接的时候会触发，如果没有触发读写操作，则客户端和客户端之间不会进行数据通信，也就是channelRead0不会执行，
         * 当通道连接的时候，触发channelActive方法向服务端发送数据触发服务器端的handler的channelRead0回调，然后
         * 服务端向客户端发送数据触发客户端的channelRead0，依次触发。
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            for (int i = 0; i < 1; i++) {
                byte[] data = ("helloworld-"+i).getBytes(StandardCharsets.UTF_8);
                RemoteCommand remoteCommand = RemoteCommand.builder().magicCode(ProtocolConstants.MAGIC_CODE).version((byte) 1).command((byte) 1).requestType(ProtocolConstants.RequestType.SYNC).correlationId(1L).timestamp(SystemClock.now()).data(data).build();
                ByteBuf byteBuf = PackageCodec.encode(remoteCommand);
                ctx.channel().writeAndFlush(byteBuf);
            }
            System.out.println("11111111");
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }

}
