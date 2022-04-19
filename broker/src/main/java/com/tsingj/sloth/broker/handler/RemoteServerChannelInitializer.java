package com.tsingj.sloth.broker.handler;

import com.tsingj.sloth.remoting.PackageDecodeHandler;
import com.tsingj.sloth.remoting.PackageEncodeHandler;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * @author yanghao
 */
public class RemoteServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final String SPLIT = "split";

    private static final String DECODER = "decoder";

    private static final String ENCODER = "encoder";

    private static final String BROKER_HANDLER = "broker_handler";

    private final int maxMessageSize;

    public RemoteServerChannelInitializer(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

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
         *      计算LengthFieldBasedFrameDecoder入参
         *      maxFrameLength = storage.maxMessageSize
         *      长度字段的offset -> lengthFieldOffset = magic_code + version + command = 7
         *      长度字段大小 -> lengthFieldLength = dataLen = 4
         */
        pipeline.addLast(SPLIT, new LengthFieldBasedFrameDecoder(maxMessageSize,
                ProtocolConstants.FieldLength.MAGIC_CODE + ProtocolConstants.FieldLength.VERSION + ProtocolConstants.FieldLength.COMMAND,
                        ProtocolConstants.FieldLength.TOTAL_LEN));

        //response dataPackage -> netty bytebuf convert
        pipeline.addLast(ENCODER, new PackageEncodeHandler());
        //receive netty bytebuf -> dataPackage convert
        pipeline.addLast(DECODER, new PackageDecodeHandler());
        //connect manager
        pipeline.addLast("channel_state", new IdleStateHandler(0, 0, 30, TimeUnit.SECONDS));
        pipeline.addLast("channel_life_cycle", LiftCycleHandler.INSTANCE);
        //process command
        pipeline.addLast(BROKER_HANDLER, new RemoteServerHandler());

    }
}
