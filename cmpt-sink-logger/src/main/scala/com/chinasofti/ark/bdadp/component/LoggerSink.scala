package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.slf4j.Logger

/**
 * Hello world!
 *
 */
class LoggerSink(id: String, name: String, log: Logger)
    extends SinkComponent[StringData](id, name, log) with Configureable with
            SparkSinkAdapter[SparkData] {

  var numRows: Int = 20

  override def apply(inputT: StringData): Unit = {
    info(inputT.getRawData)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    numRows = componentProps.getString("numRows", "20").toInt
  }

  override def apply(inputT: SparkData): Unit = {
    ("" :: inputT.getRawData.toString() ::
      Nil ++ inputT.getRawData.take(numRows)).foreach(row => info(row.toString()))
  }
}
