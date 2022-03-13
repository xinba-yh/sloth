//package com.tsingj.sloth.broker.handler;
//
//import io.netty.channel.ChannelHandler;
//import io.netty.channel.ChannelHandlerContext;
//import io.netty.channel.ChannelInboundHandlerAdapter;
//import lombok.extern.slf4j.Slf4j;
//
///**
// * @author yanghao
// */
//@Slf4j
//@ChannelHandler.Sharable
//public class PacketCodecHandler extends ChannelInboundHandlerAdapter {
//
//
//    @Override
//    public boolean isSharable() {
//        return true;
//    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        log.error("channel exceptionCaught :",cause);
//    }
//
//    @Override
//    public void channelInactive(ChannelHandlerContext ctx){
//        log.error("channel inactive...");
//    }
//
//
//    @Override
//    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
//        log.error("channel userEventTriggered...");
//    }
//
//}
