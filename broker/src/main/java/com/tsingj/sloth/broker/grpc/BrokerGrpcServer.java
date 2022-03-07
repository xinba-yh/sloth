package com.tsingj.sloth.broker.grpc;

import com.tsingj.sloth.broker.grpc.handler.MessageHandler;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author yanghao
 */
@Slf4j
public class BrokerGrpcServer {

    private final MessageHandler messageHandler;

    public BrokerGrpcServer(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    private Server server;

    public void start() throws IOException {
        /* The port on which the server should run */
        int port = 9091;
        server = ServerBuilder.forPort(port)
                .addService(new BrokerGrpcImpl(messageHandler))
                .build()
                .start();
        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    BrokerGrpcServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
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
