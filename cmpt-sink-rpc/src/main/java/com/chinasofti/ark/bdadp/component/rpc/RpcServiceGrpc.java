package com.chinasofti.ark.bdadp.component.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 * The greeting service definition.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.4.0)",
    comments = "Source: rpc.proto")
public final class RpcServiceGrpc {

  private RpcServiceGrpc() {}

  public static final String SERVICE_NAME = "rpc.RpcService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<RpcProto.RpcRequest,
      RpcProto.RpcReply> METHOD_RPC =
      io.grpc.MethodDescriptor.<com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcRequest, com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcReply>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
          .setFullMethodName(generateFullMethodName(
              "rpc.RpcService", "rpc"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcRequest.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcReply.getDefaultInstance()))
          .build();

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RpcServiceStub newStub(io.grpc.Channel channel) {
    return new RpcServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RpcServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new RpcServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RpcServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new RpcServiceFutureStub(channel);
  }

  /**
   * <pre>
   * The greeting service definition.
   * </pre>
   */
  public static abstract class RpcServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Sends a greeting
     * </pre>
     */
    public void rpc(com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcRequest request,
        io.grpc.stub.StreamObserver<RpcProto.RpcReply> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RPC, responseObserver);
    }

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_RPC,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcRequest,
                com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcReply>(
                  this, METHODID_RPC)))
          .build();
    }
  }

  /**
   * <pre>
   * The greeting service definition.
   * </pre>
   */
  public static final class RpcServiceStub extends io.grpc.stub.AbstractStub<RpcServiceStub> {
    private RpcServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RpcServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected RpcServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RpcServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a greeting
     * </pre>
     */
    public void rpc(com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcRequest request,
        io.grpc.stub.StreamObserver<RpcProto.RpcReply> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_RPC, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The greeting service definition.
   * </pre>
   */
  public static final class RpcServiceBlockingStub extends io.grpc.stub.AbstractStub<RpcServiceBlockingStub> {
    private RpcServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RpcServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected RpcServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RpcServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a greeting
     * </pre>
     */
    public java.util.Iterator<RpcProto.RpcReply> rpc(
        com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_RPC, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The greeting service definition.
   * </pre>
   */
  public static final class RpcServiceFutureStub extends io.grpc.stub.AbstractStub<RpcServiceFutureStub> {
    private RpcServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RpcServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected RpcServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RpcServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_RPC = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RpcServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RpcServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RPC:
          serviceImpl.rpc((com.chinasofti.ark.bdadp.component.rpc.RpcProto.RpcRequest) request,
              (io.grpc.stub.StreamObserver<RpcProto.RpcReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class RpcServiceDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.chinasofti.ark.bdadp.component.rpc.RpcProto.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RpcServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RpcServiceDescriptorSupplier())
              .addMethod(METHOD_RPC)
              .build();
        }
      }
    }
    return result;
  }
}
