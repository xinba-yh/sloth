package com.tsingj.sloth.broker.handler;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.PackageCodec;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import com.tsingj.sloth.remoting.utils.CommonUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author yanghao
 */
@Slf4j
public class RemoteServerHandler extends SimpleChannelInboundHandler<DataPackage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DataPackage msg) throws Exception {
        try {
            switch (msg.getCommand()) {
                case ProtocolConstants.Command.HEARTBEAT:

                    break;

                case ProtocolConstants.Command.SEND_MESSAGE:
                    this.processMessage(ctx, msg);
                    break;

                default:
                    // TODO: 2022/3/18 close channel
                    log.error("invalid command:{}!", msg.getCommand());

            }
        } catch (Exception e) {
            log.error("process command:{} exception", msg.getCommand(), e);
            if (msg.getRequestType() == ProtocolConstants.RequestType.SYNC) {
                DataPackage responseDataPackage = msg;
                responseDataPackage.setTimestamp(System.currentTimeMillis());
                responseDataPackage.setData(
                        Remoting.SendResult.newBuilder()
                                .setRetCode(Remoting.SendResult.RetCode.ERROR)
                                .setErrorInfo(CommonUtils.simpleErrorInfo(e))
                                .build()
                                .toByteArray());
                ctx.channel().writeAndFlush(responseDataPackage);
            }
        }
    }

    private void processMessage(ChannelHandlerContext ctx, DataPackage msg) throws InvalidProtocolBufferException {
        if (msg.getRequestType() == ProtocolConstants.RequestType.ONE_WAY) {
            Remoting.Message message = Remoting.Message.parseFrom(msg.getData());
            int reqId = Integer.parseInt(message.getRequestId());
            if (reqId == 1 || reqId % 10000 == 0) {
                log.info("receive command:{} oneWay reqId:{}.", msg.getCommand(), reqId);
            }
            return;
        }
        if (msg.getCorrelationId() == 1 || msg.getCorrelationId() % 10000 == 0) {
            log.info("receive command:{} correlationId:{}.", msg.getCommand(), msg.getCorrelationId());
        }
        Remoting.Message message = Remoting.Message.parseFrom(msg.getData());
        Remoting.SendResult sendResult = Remoting.SendResult.newBuilder()
                .setRetCode(Remoting.SendResult.RetCode.SUCCESS)
                .setResultInfo(Remoting.SendResult.ResultInfo.newBuilder().setOffset(msg.getCorrelationId()).setTopic(message.getTopic()).setPartition(message.getPartition()).build())
                .build();
        DataPackage responseDataPackage = msg;
        responseDataPackage.setTimestamp(System.currentTimeMillis());
        responseDataPackage.setData(sendResult.toByteArray());
        ctx.channel().writeAndFlush(responseDataPackage);
    }


}
