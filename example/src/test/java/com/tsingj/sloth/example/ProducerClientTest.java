//package com.tsingj.sloth.example;
//
//import com.tsingj.sloth.rpcmodel.grpc.protobuf.NotificationGrpc;
//import com.tsingj.sloth.rpcmodel.grpc.protobuf.NotificationOuterClass;
//import io.grpc.ManagedChannel;
//import io.grpc.ManagedChannelBuilder;
//import io.grpc.stub.StreamObserver;
//import lombok.extern.slf4j.Slf4j;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
//
//import java.util.Random;
//import java.util.concurrent.*;
//
//@Slf4j
//@RunWith(SpringJUnit4ClassRunner.class)
//public class ProducerClientTest {
//
//
//    @Test
//    public void producerPingTest() {
//        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:9091")
//                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
//                // needing certificates.
//                .usePlaintext()
//                .build();
////        ManagedChannel channel = NettyChannelBuilder.forTarget("static://localhost:9091")
////                .keepAliveTime(6, TimeUnit.MINUTES)
////                .keepAliveTimeout(2, TimeUnit.SECONDS)
////                .keepAliveWithoutCalls(true)
////                .idleTimeout(24, TimeUnit.HOURS)
////                //3秒超时
////                .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
////                .withOption(ChannelOption.SO_KEEPALIVE, true)
////                .usePlaintext()
////                .build();
//
//        NotificationGrpc.NotificationStub notificationStub = NotificationGrpc.newStub(channel);
//
//        final CountDownLatch finishLatch = new CountDownLatch(1);
//
//        StreamObserver<NotificationOuterClass.SendResult> responseObserver = new StreamObserver<NotificationOuterClass.SendResult>() {
//            @Override
//            public void onNext(NotificationOuterClass.SendResult sendResult) {
//                log.info("receive new ack:{}", sendResult.toString());
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                log.error("error:{}", throwable.getMessage());
//                finishLatch.countDown();
//            }
//
//            @Override
//            public void onCompleted() {
//                log.info("finished onCompleted");
//                finishLatch.countDown();
//            }
//        };
//
//        StreamObserver<NotificationOuterClass.SendRequest> requestObserver = notificationStub.send(responseObserver);
//        try {
//            for (int i = 0; i < 10; i++) {
//                NotificationOuterClass.SendRequest request = NotificationOuterClass.SendRequest.newBuilder().setRequestType(NotificationOuterClass.SendRequest.SendRequestType.PING).setPing(NotificationOuterClass.Ping.newBuilder().setPing("ping" + i).build()).build();
//                requestObserver.onNext(request);
//            }
//        } catch (Throwable e) {
//            requestObserver.onError(e);
//        } finally {
//            requestObserver.onCompleted();
//        }
//
//        try {
//            finishLatch.await(2, TimeUnit.SECONDS);
//        } catch (InterruptedException ignored) {
//        }
//    }
//
//
//    @Test
//    public void producerMessageTest() {
////        for (int j = 0; j < 200; j++) {
//            long startTime = System.currentTimeMillis();
//            int defaultProducerCount = 4;
//            CountDownLatch countDownLatch = new CountDownLatch(defaultProducerCount);
//            int producerMessageCount = 50000;
//            for (int i = 0; i < defaultProducerCount; i++) {
//                Thread thread = new Thread(new ProducerClient(producerMessageCount, countDownLatch));
//                thread.start();
//            }
//            try {
//                countDownLatch.await(60, TimeUnit.SECONDS);
//                log.info("sendAndReceive:{},cost:{} ms", defaultProducerCount * producerMessageCount, System.currentTimeMillis() - startTime);
//            } catch (InterruptedException e) {
//                log.warn("wait producer timeout!");
//            }
////        }
//    }
//
//    @Test
//    public void futureTest() {
//        ExecutorService executorService = Executors.newFixedThreadPool(9);
//        for (int i = 0; i < 20; i++) {
//            CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(ProducerClientTest::helloworld, executorService);
//            completableFuture.exceptionally(throwable -> {
//                throwable.printStackTrace();
//                return null;
//            });
//            try {
//                String s = completableFuture.get(200, TimeUnit.MILLISECONDS);
//                System.out.println(s);
//            } catch (Exception e) {
//                System.out.println("timeout.");
//            }
//        }
//    }
//
//    static String helloworld() {
//        int i = (int) (Math.random() * 500);
//        System.out.println(Thread.currentThread().getName() + "  " + i);
//        if (i > 200) {
//            throw new RuntimeException("chaoshile");
//        }
//        return "helloworld";
//    }
//
//
//}
