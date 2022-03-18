package com.tsingj.sloth.broker.handler;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.DataPackage;
import com.tsingj.sloth.remoting.protocol.PackageCodec;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
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

    }

    private void processMessage(ChannelHandlerContext ctx, DataPackage msg) throws InvalidProtocolBufferException {
        if(msg.getRequestType() == ProtocolConstants.RequestType.ONE_WAY){
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
        DataPackage responseDataPackage = this.buildSendResultResponse(msg, sendResult);
        ctx.channel().writeAndFlush(responseDataPackage);
    }

    private DataPackage buildSendResultResponse(DataPackage msg, Remoting.SendResult sendResult) {
        return DataPackage.builder()
                .magicCode(ProtocolConstants.MAGIC_CODE)
                .command(msg.getCommand())
                .version(msg.getVersion())
                .requestType(msg.getRequestType())
                .correlationId(msg.getCorrelationId())
                .timestamp(System.currentTimeMillis())
                .data(sendResult.toByteArray())
                .build();
    }

}
