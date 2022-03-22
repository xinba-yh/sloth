package com.tsingj.sloth.remoting;

import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.PackageCodec;
import com.tsingj.sloth.remoting.utils.CommonUtils;
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
            ByteBuf encode = PackageCodec.encode(msg);
            log.debug("encode length:{}", encode.readableBytes());
            out.writeBytes(encode);
        } catch (Exception e) {
            log.error("encode exception, {}", msg.toString(), e);
            CommonUtils.closeChannel(ctx.channel(), "encode error!");
        }
    }

}
