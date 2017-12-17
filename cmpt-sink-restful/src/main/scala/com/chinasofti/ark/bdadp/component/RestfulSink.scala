package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.sink.SinkComponent
import org.json4s.jackson
import org.slf4j.Logger

import scala.collection.immutable

class RestfulSink(id: String, name: String, log: Logger)
  extends SinkComponent[SparkData](id, name, log) with Configureable {

  var host: String = _
  var port: Int = _
  var uri: String = _

  override def configure(componentProps: ComponentProps): Unit = {
    host = componentProps.getString("host", "localhost")
    port = componentProps.getInt("port", 27001)
    uri = componentProps.getString("path", "/")

  }

  override def apply(inputT: SparkData): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val transform: () => (RDD[Map[String, Any]]) = () => {
      inputT.getRawData.map(row => {
        row.schema.indices.map(i => {
          (row.schema.fieldNames(i), row.get(i))
        }).toMap
      })
    }

    val response: () => (HttpResponse) = () => {
      val data = transform().collect()
      info("data.length -> " + data.length)
      val json = jackson.Json(DefaultFormats).write(data)
      info("data.json -> " + json)
      HttpResponse(
        headers = immutable.Seq(AccessControlAllowOrigin.create(HttpOriginRange.*)),
        entity = HttpEntity(ContentTypes.`application/json`, json))
    }

    val handler: HttpRequest => HttpResponse = {
      case HttpRequest(HttpMethods.GET, Uri.Path(uri), _, _, _) => response()
      case HttpRequest(HttpMethods.OPTIONS, Uri.Path(uri), _, _, _) =>
        HttpResponse(headers = immutable.Seq(
          AccessControlAllowOrigin.create(HttpOriginRange.*),
          AccessControlAllowMethods.create(HttpMethods.GET),
          AccessControlAllowHeaders.create("X-Custom-Header"),
          AccessControlAllowCredentials.create(true),
          AccessControlMaxAge.create(1800)
        ))
      case _: HttpRequest => HttpResponse(404)
    }


    Http().bindAndHandleSync(handler, host, port)
  }

}
