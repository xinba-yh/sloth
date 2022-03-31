package com.tsingj.sloth.client;

import com.tsingj.sloth.remoting.ResponseFuture;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;


/**
 * @author yanghao
 */
@Slf4j
public class RemoteClientHandler extends SimpleChannelInboundHandler<DataPackage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DataPackage dataPackage) throws Exception {
        Long correlationId = dataPackage.getCorrelationId();
        byte[] responseData = dataPackage.getData();
        ResponseFuture responseFuture = RemoteCorrelationManager.CORRELATION_ID_RESPONSE_MAP.get(correlationId);
        if (responseFuture == null) {
            log.warn("invalid correlationId:{}!", correlationId);
            return;
        }
        if (responseData == null) {
            log.warn("correlationId:{} response null!", correlationId);
            return;
        }
        responseFuture.putResponse(dataPackage);
//        log.debug("client receive correlationId:{} data:{}", correlationId, Remoting.SendResult.parseFrom(responseData));
    }

}
