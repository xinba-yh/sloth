package com.tsingj.sloth.remoting;

import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.PackageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author yanghao
 */
@Slf4j
public class PackageEncodeHandler extends MessageToByteEncoder<DataPackage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, DataPackage msg, ByteBuf out) throws Exception {
        try {
            out.writeBytes(PackageCodec.encode(msg));
        } catch (Exception e) {
            log.error("encode exception, {}", msg.toString(), e);
            ctx.channel().close().addListener((ChannelFutureListener) future -> log.info("closeChannel: close the connection result: {}", future.isSuccess()));
        }
    }

}
