package com.tsingj.sloth.broker.handler;

import com.tsingj.sloth.broker.handler.processor.RemoteRequestProcessorSelector;
import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.remoting.message.Remoting;
import com.tsingj.sloth.remoting.protocol.RemoteCommand;
import com.tsingj.sloth.remoting.protocol.ProtocolConstants;
import com.tsingj.sloth.remoting.utils.CommonUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yanghao
 */
@Slf4j
public class RemoteServerHandler extends SimpleChannelInboundHandler<RemoteCommand> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemoteCommand msg) throws Exception {
        try {
            switch (msg.getCommand()) {
                case ProtocolConstants.Command.PRODUCER_HEARTBEAT:

                    break;

                case ProtocolConstants.Command.SEND_MESSAGE:
                    this.processSendMessage(ctx, msg);
                    break;

                case ProtocolConstants.Command.GET_MESSAGE:
                    this.processGetMessage(ctx, msg, ProtocolConstants.Command.GET_MESSAGE);
                    break;

                case ProtocolConstants.Command.GET_MAX_OFFSET:
                    this.processGetMessage(ctx, msg, ProtocolConstants.Command.GET_MAX_OFFSET);
                    break;

                case ProtocolConstants.Command.GET_MIN_OFFSET:
                    this.processGetMessage(ctx, msg, ProtocolConstants.Command.GET_MIN_OFFSET);
                    break;

                case ProtocolConstants.Command.CONSUMER_GROUP_HEARTBEAT:
                    this.processConsumerGroupRequest(ctx, msg, ProtocolConstants.Command.CONSUMER_GROUP_HEARTBEAT);
                    break;

                case ProtocolConstants.Command.GET_CONSUMER_GROUP_OFFSET:
                    this.processConsumerGroupRequest(ctx, msg, ProtocolConstants.Command.GET_CONSUMER_GROUP_OFFSET);
                    break;

                case ProtocolConstants.Command.SUBMIT_CONSUMER_GROUP_OFFSET:
                    this.processConsumerGroupRequest(ctx, msg, ProtocolConstants.Command.SUBMIT_CONSUMER_GROUP_OFFSET);
                    break;

                default:
                    // TODO: 2022/3/18 close channel
                    log.error("invalid command:{}!", msg.getCommand());
                    CommonUtils.closeChannel(ctx.channel(), "invalid command:" + msg.getCommand());
            }
        } catch (Exception e) {
            log.error("process command:{} exception", msg.getCommand(), e);
            if (msg.getRequestType() == ProtocolConstants.RequestType.SYNC) {
                RemoteCommand responseRemoteCommand = msg;
                responseRemoteCommand.setTimestamp(SystemClock.now());
                responseRemoteCommand.setData(
                        Remoting.SendResult.newBuilder()
                                .setRetCode(Remoting.SendResult.RetCode.ERROR)
                                .setErrorInfo(CommonUtils.simpleErrorInfo(e))
                                .build()
                                .toByteArray());
                ctx.channel().writeAndFlush(responseRemoteCommand);
            }
        }
    }

    private void processSendMessage(ChannelHandlerContext ctx, RemoteCommand request) throws Exception {
        RemoteCommand response;
        if (request.getRequestType() == ProtocolConstants.RequestType.ONE_WAY) {
            Remoting.Message message = Remoting.Message.parseFrom(request.getData());
            int reqId = Integer.parseInt(message.getRequestId());
            if (reqId == 1 || reqId % 10000 == 0) {
                log.info("receive command:{} oneWay reqId:{}.", request.getCommand(), reqId);
            }
            RemoteRequestProcessorSelector.select(ProtocolConstants.Command.SEND_MESSAGE).process(request, ctx);
        } else {
            if (request.getCorrelationId() == 1 || request.getCorrelationId() % 10000 == 0) {
                log.info("receive command:{} correlationId:{}.", request.getCommand(), request.getCorrelationId());
            }
            //only sync response msg.
            response = RemoteRequestProcessorSelector.select(ProtocolConstants.Command.SEND_MESSAGE).process(request, ctx);
            ctx.channel().writeAndFlush(response);
        }
    }

    private void processGetMessage(ChannelHandlerContext ctx, RemoteCommand request, byte command) throws Exception {
        RemoteCommand response = RemoteRequestProcessorSelector.select(command).process(request, ctx);
        ctx.channel().writeAndFlush(response);
    }


    private void processConsumerGroupRequest(ChannelHandlerContext ctx, RemoteCommand request, byte command) throws Exception {
        RemoteCommand response = RemoteRequestProcessorSelector.select(command).process(request, ctx);
        ctx.channel().writeAndFlush(response);
    }


}
