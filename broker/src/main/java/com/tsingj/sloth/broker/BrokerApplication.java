package com.tsingj.sloth.broker;

import com.tsingj.sloth.broker.grpc.BrokerGrpcServer;
import com.tsingj.sloth.broker.grpc.handler.MessageHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.util.Assert;

import java.io.IOException;

/**
 * @author yanghao
 */
@ComponentScan(basePackages = "com.tsingj.sloth")
@SpringBootApplication
public class BrokerApplication {

    public static void main(String[] args) throws IOException {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(BrokerApplication.class, args);
        MessageHandler messageHandler = applicationContext.getBean(MessageHandler.class);
        Assert.notNull(messageHandler, "missing messageHandler!");
        BrokerGrpcServer brokerGrpcServer = new BrokerGrpcServer(messageHandler);
        brokerGrpcServer.start();
    }

}
