package com.tsingj.sloth.client;

import com.google.protobuf.ByteString;
import com.tsingj.sloth.client.producer.SlothRemoteProducer;
import com.tsingj.sloth.client.springsupport.SlothClientProperties;
import com.tsingj.sloth.remoting.message.Remoting;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import java.util.concurrent.atomic.AtomicLong;


@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
public class SlothProducerTest {

    @Autowired
    private SlothRemoteProducer slothProducer;

    /**
     * 1S 10W server收到
     */
    @Test
    public void sendOneWayTest() throws InterruptedException {

        AtomicLong ID = new AtomicLong(1);
        int threadCount = 4;
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    long reqId = ID.getAndAdd(1);
                    Remoting.Message.Builder builder = Remoting.Message.newBuilder();
                    builder.setBody(ByteString.copyFromUtf8(" PutMessageResult putMessageResult = storageEngine.putMessage(message);\n" +
                            "//        if (putMessageResult.getStatus() == PutMessageStatus.OK) {\n" +
                            "//            return NotificationOuterClass.SendResult.newBuilder()\n" +
                            "//                    .setResponseType(NotificationOuterClass.SendResult.SendResponseType.ACK)\n" +
                            "//                    .setAck(NotificationOuterClass.SendResult.Ack.newBuilder()\n" +
                            "//                            .setRetCode(NotificationOuterClass.SendResult.Ack.RetCode.SUCCESS)\n" +
                            "//                            .setRequestId(requestId)\n" +
                            "//                            .setResultInfo(NotificationOuterClass.SendResult.Ack.ResultInfo.newBuilder()\n" +
                            "//                                    .setTopic(topic)\n" +
                            "//                                    .setPartition(partition)\n" +
                            "//                                    .setOffset(putMessageResult.getOffset())\n" +
                            "//                                    .build())\n" +
                            "//                            .build()).build();\n" +
                            "//        } else {\n" +
                            "//            return this.respError(requestId, putMessageResult.getStatus() + \":\" + putMessageResult.getErrorMsg());\n" +
                            "//        }\n" +
                            "//    }------------------" + reqId));
                    builder.setTopic("test-topic");
                    builder.setPartition(1);
                    builder.setRequestId("" + reqId);
                    slothProducer.sendOneway(builder.build());
                }
            }).start();
        }
        Thread.sleep(5000);

    }

    /**
     * 1S 4W
     * 单client、多client性能一致。
     * @throws InterruptedException
     */
    @Test
    public void sendSyncResponseTest() throws InterruptedException {

        int threadCount = 4;
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                int count = 10;
                StopWatch stopWatch = new StopWatch();
                for (int j = 0; j < count; j++) {
                    stopWatch.start();
                    Remoting.Message.Builder builder = Remoting.Message.newBuilder();
                    builder.setBody(ByteString.copyFromUtf8(" PutMessageResult putMessageResult = storageEngine.putMessage(message);\n" +
                            "//        if (putMessageResult.getStatus() == PutMessageStatus.OK) {\n" +
                            "//            return NotificationOuterClass.SendResult.newBuilder()\n" +
                            "//                    .setResponseType(NotificationOuterClass.SendResult.SendResponseType.ACK)\n" +
                            "//                    .setAck(NotificationOuterClass.SendResult.Ack.newBuilder()\n" +
                            "//                            .setRetCode(NotificationOuterClass.SendResult.Ack.RetCode.SUCCESS)\n" +
                            "//                            .setRequestId(requestId)\n" +
                            "//                            .setResultInfo(NotificationOuterClass.SendResult.Ack.ResultInfo.newBuilder()\n" +
                            "//                                    .setTopic(topic)\n" +
                            "//                                    .setPartition(partition)\n" +
                            "//                                    .setOffset(putMessageResult.getOffset())\n" +
                            "//                                    .build())\n" +
                            "//                            .build()).build();\n" +
                            "//        } else {\n" +
                            "//            return this.respError(requestId, putMessageResult.getStatus() + \":\" + putMessageResult.getErrorMsg());\n" +
                            "//        }\n" +
                            "//    }------------------" + j));
                    builder.setTopic("test-topic");
//                    builder.setPartition(1);
                    Remoting.SendResult sendResult = slothProducer.send(builder.build());
                    if (sendResult.getRetCode() != Remoting.SendResult.RetCode.SUCCESS) {
                        log.warn("sync response:{}", sendResult);
                    }
                    stopWatch.stop();
                }
                log.info("sendSync count:{} take:{} avg:{}", count, stopWatch.getTotalTimeMillis(), stopWatch.getTotalTimeMillis() / count);
            }).start();
        }

        Thread.sleep(20000);

    }

}
