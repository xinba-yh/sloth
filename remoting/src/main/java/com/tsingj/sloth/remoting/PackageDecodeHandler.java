package com.tsingj.sloth.remoting;

import com.tsingj.sloth.remoting.protocol.PackageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author yanghao
 */
@Slf4j
public class PackageDecodeHandler extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Object decoded = PackageCodec.decode(in);
        if (decoded != null) {
            out.add(decoded);
        }
    }

}
