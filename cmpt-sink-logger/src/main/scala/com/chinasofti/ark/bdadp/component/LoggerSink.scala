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
            SparkSinkAdapter[SparkData] with Serializable {

  var numRows: Int = 20
  var numPartitions: Int = 1

  override def apply(inputT: StringData): Unit = {
    info(inputT.getRawData)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    numRows = componentProps.getString("numRows", "20").toInt
    numPartitions = componentProps.getString("numPartitions", "1").toInt
  }

  override def apply(inputT: SparkData): Unit = {
    info(inputT.getRawData.toString())
    inputT.getRawData.limit(numRows).repartition(numPartitions)
        .collect().foreach(row => info(row.toString()))
  }
}
