package com.tsingj.sloth.example;

import com.google.protobuf.ByteString;
import com.tsingj.sloth.rpcmodel.grpc.protobuf.NotificationGrpc;
import com.tsingj.sloth.rpcmodel.grpc.protobuf.NotificationOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ProducerClient implements Runnable {

    private final int producerMessageCount;

    private final CountDownLatch countDownLatch;

    public ProducerClient(int producerMessageCount, CountDownLatch countDownLatch) {
        this.producerMessageCount = producerMessageCount;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        ManagedChannel channel = NettyChannelBuilder.forTarget("localhost:9091")
                .keepAliveTime(6, TimeUnit.MINUTES)
                .keepAliveTimeout(2, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .idleTimeout(24, TimeUnit.HOURS)
                //3秒超时
                .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)
                .withOption(ChannelOption.SO_KEEPALIVE, true)
                .usePlaintext()
                .build();
        NotificationGrpc.NotificationStub notificationStub = NotificationGrpc.newStub(channel);

        final Object lock = new Object();
        final CountDownLatch finishLatch = new CountDownLatch(1);
        final AtomicLong ackCount = new AtomicLong();


        final StreamObserver<NotificationOuterClass.SendResult> responseObserver = new StreamObserver<NotificationOuterClass.SendResult>() {
            @Override
            public void onNext(NotificationOuterClass.SendResult sendResult) {
                synchronized (lock) {
                    lock.notify();
                }
                long currentAckCount = ackCount.addAndGet(1);
                if (currentAckCount % 10000 == 0) {
                    log.info("receive count:{} new ack:{}", currentAckCount, sendResult.toString());
                } else {
                    NotificationOuterClass.SendResult.Ack ack = sendResult.getAck();
                    if (ack.getRetCode() == NotificationOuterClass.SendResult.Ack.RetCode.ERROR) {
                        log.warn("ack error:{}", ack.getInfo());
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("error:{}", throwable.getMessage());
                synchronized (lock) {
                    lock.notify();
                }
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                log.info("finished onCompleted");
                synchronized (lock) {
                    lock.notify();
                }
                finishLatch.countDown();
            }
        };

        StreamObserver<NotificationOuterClass.SendRequest> requestObserver = notificationStub.send(responseObserver);
        try {
            for (int i = 0; i < this.producerMessageCount; i++) {
                long startTime = System.currentTimeMillis();
                String body = "hello world! " + i;
                NotificationOuterClass.SendRequest request = NotificationOuterClass.SendRequest.newBuilder()
                        .setRequestType(NotificationOuterClass.SendRequest.SendRequestType.MESSAGE)
                        .setMsg(NotificationOuterClass.SendRequest.Message.newBuilder()
                                .setRequestId((i + 1) + "")
                                .setTopic("test-topic-1")
                                .setBody(ByteString.copyFrom(body.getBytes(StandardCharsets.UTF_8)))
                                .setAck(true) //unused
                                .build())
                        .build();
                requestObserver.onNext(request);
                //why faster need sleep!  netty grpc - Failed to get SOMAXCONN from sysctl and file
                synchronized (lock) {
                    lock.wait(5000);
                }
                if (i % 10000 == 0) {
                    System.out.println("----------------send " + i + "---------------");
                }
                long takeTime = System.currentTimeMillis() - startTime;
                if (takeTime > 100) {
                    System.out.println("send slow!" + takeTime + " ms");
                }
            }
            System.out.println("------------------------------------------------");
        } catch (Throwable e) {
            e.printStackTrace();
            requestObserver.onError(e);
        }
        requestObserver.onCompleted();
        try {
            finishLatch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        } finally {
            countDownLatch.countDown();
        }
    }
}
