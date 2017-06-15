package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{ SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SparkSinkAdapter, SinkComponent}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

/**
 * Created by Administrator on 2017.2.16.
 */
class CSVSink (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] {

  var path: String = null
  var header: String = null
  var delimiter: String = null
  var quote: String = null
  var escape: String = null
  var nullValue: String = null
  var dateFormat: String = null
  var codec: String = null
  var numPartitions: Int = 0


  override def apply(inputT: StringData): Unit = {
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    header = componentProps.getString("header", "false")
    delimiter = componentProps.getString("delimiter", ",")
    quote = componentProps.getString("quote", "\"")
    escape = componentProps.getString("escape", "\\")
    nullValue = componentProps.getString("nullValue")
    dateFormat = componentProps.getString("dateFormat")
    codec = componentProps.getString("codec")
    numPartitions = componentProps.getInt("numPartitions",8)

    StringUtils.assertIsBlank(path);
  }

  override def apply(inputT: SparkData): Unit = {
    inputT.getRawData.repartition(numPartitions).write.format("com.databricks.spark.csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .option("quote", quote)
      .option("escape", escape)
      .option("codec", codec)
      .option("nullValue", nullValue)
      .option("dateFormat", dateFormat).save(path)

  }
}
