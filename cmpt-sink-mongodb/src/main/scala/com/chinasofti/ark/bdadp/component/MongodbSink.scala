package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.sink.SinkComponent
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.slf4j.Logger

/**
  * Created by Administrator on 2017.2.16.
  */
class MongodbSink(id: String, name: String, log: Logger)
  extends SinkComponent[SparkData](id, name, log) with Configureable with Serializable {

  var numPartitions = 1

  var uri: String = _
  var userName: String = _
  var password: String = _
  var database: String = _
  var collection: String = _
  var localThreshold: String = _
  var replaceDocument: String = _
  var maxBatchSize: String = _
  var mode: String = _

  override def configure(componentProps: ComponentProps): Unit = {
    numPartitions = componentProps.getInt("numPartitions", 1)

    uri = componentProps.getString("uri")
    userName = componentProps.getString("userName", "")
    password = componentProps.getString("password")
    database = componentProps.getString("database")
    collection = componentProps.getString("collection")
    localThreshold = componentProps.getString("localThreshold", "15")
    replaceDocument = componentProps.getString("replaceDocument", "true")
    maxBatchSize = componentProps.getString("maxBatchSize", "512")
    mode = componentProps.getString("mode", "override")

  }

  override def apply(inputT: SparkData): Unit = {
    val dataFrame = inputT.getRawData.repartition(numPartitions)
    val writeConfig = WriteConfig(Map(
      "uri" -> uri,
      "database" -> database,
      "collection" -> collection,
      "localThreshold" -> localThreshold,
      "replaceDocument" -> replaceDocument,
      "maxBatchSize" -> maxBatchSize
    ))

    MongoSpark.save(dataFrame, writeConfig)
  }
}
