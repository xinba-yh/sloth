package com.tsingj.sloth.broker.grpc;

import com.tsingj.sloth.broker.grpc.handler.MessageHandler;
import com.tsingj.sloth.broker.grpc.protobuf.NotificationGrpc;
import com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass;
import com.tsingj.sloth.store.pojo.Result;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * @author yanghao
 */

@Slf4j
@GrpcService
public class BrokerController extends NotificationGrpc.NotificationImplBase {

    private final MessageHandler messageHandler;

    public BrokerController(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    @Override
    public StreamObserver<NotificationOuterClass.SendRequest> send(StreamObserver<NotificationOuterClass.SendResult> resp) {

        StreamObserver<NotificationOuterClass.SendRequest> streamObserver = new StreamObserver<NotificationOuterClass.SendRequest>() {
            @Override
            public void onNext(NotificationOuterClass.SendRequest request) {
                switch (request.getRequestType()) {
                    case PING:
                        NotificationOuterClass.Pong pong = NotificationOuterClass.Pong.newBuilder().setPong("pong").build();
                        resp.onNext(NotificationOuterClass.SendResult.newBuilder()
                                .setResponseType(NotificationOuterClass.SendResult.SendResponseType.PONG)
                                .setPong(pong)
                                .build());
                        break;
                    case MESSAGE:
                        NotificationOuterClass.SendRequest.Message msg = request.getMsg();
                        Result result = messageHandler.storeMessage(msg);
                        String messageId = msg.getMessageId();
                        NotificationOuterClass.SendResult.Ack ack;
                        if (result.success()) {
                            ack = NotificationOuterClass.SendResult.Ack.newBuilder()
                                    .setRetCode(NotificationOuterClass.SendResult.Ack.RetCode.SUCCESS)
                                    .setMessageId(messageId)
                                    .build();
                            resp.onNext(NotificationOuterClass.SendResult.newBuilder().setAck(ack).build());
                        } else {
                            ack = NotificationOuterClass.SendResult.Ack.newBuilder()
                                    .setRetCode(NotificationOuterClass.SendResult.Ack.RetCode.ERROR)
                                    .setMessageId(messageId)
                                    .build();
                        }
                        resp.onNext(NotificationOuterClass.SendResult.newBuilder()
                                .setResponseType(NotificationOuterClass.SendResult.SendResponseType.ACK)
                                .setAck(ack)
                                .build());
                        break;
                    default:
                        throw new UnsupportedOperationException("invalid requestType!");
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Encountered error in sendMessage.", t);
                resp.onError(t);
            }

            @Override
            public void onCompleted() {
                log.info("send sendMessage onCompleted.");
                resp.onCompleted();
            }

        };
        return streamObserver;
    }


}
