package com.tsingj.sloth.broker.handler;

import com.tsingj.sloth.broker.handler.protocol.PackageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * @author yanghao
 */
public class PackageDecodeHandler extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Object decoded = PackageCodec.decode(in);
        if (decoded != null) {
            out.add(decoded);
        }
    }


}
