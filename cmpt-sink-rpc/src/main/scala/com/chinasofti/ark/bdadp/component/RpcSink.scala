package com.chinasofti.ark.bdadp.component

import java.net.InetSocketAddress

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.sink.SinkComponent
import org.slf4j.Logger

class RpcSink(id: String, name: String, log: Logger)
  extends SinkComponent[SparkData](id, name, log) with Configureable {

  var host: String = _
  var port: Int = _
  var interval: Int = _

  override def configure(componentProps: ComponentProps): Unit = {
    host = componentProps.getString("host", "localhost")
    port = componentProps.getInt("port", 27002)
    interval = componentProps.getInt("interval", 5)

  }

  override def apply(inputT: SparkData): Unit = {
    val address = new InetSocketAddress(host, port)
    val server = NettyServerBuilder.forAddress(address)
      .addService(new RpcServiceImpl(inputT))
      .build()
      .start()
    info("Server started, listening on " + port)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        info("*** shutting down gRPC server since JVM is shutting down")
        if (server != null) {
          server.shutdown()
        }
        info("*** server shut down")
      }
    })
  }

  class RpcServiceImpl(inputT: SparkData)
    extends RpcServiceGrpc.RpcServiceImplBase with Serializable {

    /**
      * <pre>
      * Sends a greeting
      * </pre>
      */
    override def rpc(request: RpcProto.RpcRequest,
      responseObserver: StreamObserver[RpcProto.RpcReply]): Unit = {
      info("request -> " + request)
      var prev = inputT.getRawData.collect()
      while (true) {
        val curr = inputT.getRawData.collect()
        val data = curr.filter(c => !prev.exists(p => p.equals(c))).map(row => {
          val replyBuilder = RpcProto.RpcReply.newBuilder()
          row.schema.indices.foreach(i => {
            val field = RpcProto.RpcReply.getDescriptor.findFieldByNumber(i + 1)
            val value = row.get(i)
            replyBuilder.setField(field, value)
          })
          replyBuilder.build()
        })

        info("data.length -> " + data.length)
        data.foreach(v => responseObserver.onNext(v))

        //        prev.unpersist(blocking = false)
        prev = curr

        Thread.sleep(1000 * interval)
      }

      responseObserver.onCompleted()
    }
  }

}
