package com.tsingj.sloth.broker.handler;

import com.tsingj.sloth.remoting.utils.CommonUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yanghao
 */
@Slf4j
@ChannelHandler.Sharable
public class LiftCycleHandler extends ChannelInboundHandlerAdapter {

    public static final LiftCycleHandler INSTANCE = new LiftCycleHandler();

    @Override
    public boolean isSharable() {
        return true;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("LiftCycle: exceptionCaught exception.", cause);
        CommonUtils.closeChannel(ctx.channel(), cause.getMessage());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("LiftCycle: channel inactive.");
        super.channelInactive(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("LiftCycle: channel active.");
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                CommonUtils.closeChannel(ctx.channel(), "LiftCycle: IDLE exception");
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

}
