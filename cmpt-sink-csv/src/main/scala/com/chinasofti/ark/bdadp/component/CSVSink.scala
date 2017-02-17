package com.chinasofti.ark.bdadp.component

import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SparkSinkAdapter, SinkComponent}
import org.slf4j.Logger

/**
 * Created by Administrator on 2017.2.16.
 */
class CSVSink (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] {

  var path: String = null

  override def apply(inputT: StringData): Unit = {
    //    info(inputT.getRawData)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
  }

  override def apply(inputT: SparkData): Unit = {
    inputT.getRawData.write.format("com.databricks.spark.csv").save(path)

  }
}
