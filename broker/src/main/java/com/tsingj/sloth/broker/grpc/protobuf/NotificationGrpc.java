package com.tsingj.sloth.broker.grpc.protobuf;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.42.2)",
    comments = "Source: notification.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class NotificationGrpc {

  private NotificationGrpc() {}

  public static final String SERVICE_NAME = "Notification";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendRequest,
      com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult> getSendMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Send",
      requestType = com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendRequest.class,
      responseType = com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendRequest,
      com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult> getSendMethod() {
    io.grpc.MethodDescriptor<com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendRequest, com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult> getSendMethod;
    if ((getSendMethod = NotificationGrpc.getSendMethod) == null) {
      synchronized (NotificationGrpc.class) {
        if ((getSendMethod = NotificationGrpc.getSendMethod) == null) {
          NotificationGrpc.getSendMethod = getSendMethod =
              io.grpc.MethodDescriptor.<com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendRequest, com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Send"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult.getDefaultInstance()))
              .setSchemaDescriptor(new NotificationMethodDescriptorSupplier("Send"))
              .build();
        }
      }
    }
    return getSendMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static NotificationStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NotificationStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NotificationStub>() {
        @java.lang.Override
        public NotificationStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NotificationStub(channel, callOptions);
        }
      };
    return NotificationStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static NotificationBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NotificationBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NotificationBlockingStub>() {
        @java.lang.Override
        public NotificationBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NotificationBlockingStub(channel, callOptions);
        }
      };
    return NotificationBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static NotificationFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NotificationFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NotificationFutureStub>() {
        @java.lang.Override
        public NotificationFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NotificationFutureStub(channel, callOptions);
        }
      };
    return NotificationFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class NotificationImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendRequest> send(
        io.grpc.stub.StreamObserver<com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getSendMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSendMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendRequest,
                com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult>(
                  this, METHODID_SEND)))
          .build();
    }
  }

  /**
   */
  public static final class NotificationStub extends io.grpc.stub.AbstractAsyncStub<NotificationStub> {
    private NotificationStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NotificationStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NotificationStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendRequest> send(
        io.grpc.stub.StreamObserver<com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getSendMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   */
  public static final class NotificationBlockingStub extends io.grpc.stub.AbstractBlockingStub<NotificationBlockingStub> {
    private NotificationBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NotificationBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NotificationBlockingStub(channel, callOptions);
    }
  }

  /**
   */
  public static final class NotificationFutureStub extends io.grpc.stub.AbstractFutureStub<NotificationFutureStub> {
    private NotificationFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NotificationFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NotificationFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_SEND = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final NotificationImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(NotificationImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.send(
              (io.grpc.stub.StreamObserver<com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.SendResult>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class NotificationBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    NotificationBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.tsingj.sloth.broker.grpc.protobuf.NotificationOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Notification");
    }
  }

  private static final class NotificationFileDescriptorSupplier
      extends NotificationBaseDescriptorSupplier {
    NotificationFileDescriptorSupplier() {}
  }

  private static final class NotificationMethodDescriptorSupplier
      extends NotificationBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    NotificationMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (NotificationGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new NotificationFileDescriptorSupplier())
              .addMethod(getSendMethod())
              .build();
        }
      }
    }
    return result;
  }
}
