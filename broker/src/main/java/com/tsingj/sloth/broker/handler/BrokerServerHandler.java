package com.tsingj.sloth.broker.handler;

import com.tsingj.sloth.broker.handler.protocol.DataPackage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.springframework.stereotype.Component;

/**
 * @author yanghao
 */
@Component
public class BrokerServerHandler extends SimpleChannelInboundHandler<DataPackage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DataPackage msg) throws Exception {
        System.out.println("receive:" + new String(msg.getData()));
    }
}
