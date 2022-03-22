package com.tsingj.sloth.client;

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
public class RemoteClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final String SPLIT = "split";

    private static final String DECODER = "decoder";

    private static final String ENCODER = "encoder";

    private final int maxMessageSize;

    public RemoteClientChannelInitializer(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    private static final String CLIENT = "client_handler";

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(SPLIT, new LengthFieldBasedFrameDecoder(maxMessageSize,
                ProtocolConstants.FieldLength.MAGIC_CODE + ProtocolConstants.FieldLength.VERSION + ProtocolConstants.FieldLength.COMMAND,
                ProtocolConstants.FieldLength.TOTAL_LEN));
        pipeline.addLast(ENCODER, new PackageEncodeHandler());
        pipeline.addLast(DECODER, new PackageDecodeHandler());
        pipeline.addLast(CLIENT, new RemoteClientHandler());
    }

}
