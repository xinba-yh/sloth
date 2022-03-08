package com.tsingj.sloth.broker;

import com.tsingj.sloth.broker.grpc.BrokerGrpcImpl;
import com.tsingj.sloth.broker.grpc.handler.MessageHandler;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

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
        Integer grpcPort = applicationContext.getEnvironment().getProperty("grpc.server.port", Integer.class, 9091);
        BrokerGrpcServer brokerGrpcServer = new BrokerGrpcServer(messageHandler);
        brokerGrpcServer.start(grpcPort);
    }

    /**
     * @author yanghao
     */
    @Slf4j
    public static class BrokerGrpcServer {

        private final MessageHandler messageHandler;

        public BrokerGrpcServer(MessageHandler messageHandler) {
            this.messageHandler = messageHandler;
        }

        private Server server;

        public void start(Integer grpcPort) throws IOException {
            /* The port on which the server should run */
            server = ServerBuilder.forPort(grpcPort)
                    .addService(new BrokerGrpcImpl(messageHandler))
                    .build()
                    .start();
            log.info("Grpc server started, listening on " + grpcPort);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    BrokerGrpcServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }));
        }

        private void stop() throws InterruptedException {
            if (server != null) {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            }
        }

        /**
         * Await termination on the main thread since the grpc library uses daemon threads.
         */
        private void blockUntilShutdown() throws InterruptedException {
            if (server != null) {
                server.awaitTermination();
            }
        }

    }

}
