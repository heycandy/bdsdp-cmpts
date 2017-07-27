package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

/**
  * Created by Administrator on 2017.2.16.
  */
class CSVSink(id: String, name: String, log: Logger)
    extends SinkComponent[StringData](id, name, log) with Configureable with
            SparkSinkAdapter[SparkData] {

  var numPartitions = 0
  var path: String = _
  var header: String = _
  var delimiter: String = _
  var quote: String = _
  var escape: String = _
  var charset: String = _
  var nullValue: String = _
  var dateFormat: String = _
  var codec: String = _
  var quoteMode: String = _

  override def apply(inputT: StringData): Unit = {
  }

  override def configure(componentProps: ComponentProps): Unit = {
    numPartitions = componentProps.getInt("numPartitions", 8)

    path = componentProps.getString("path")
    header = componentProps.getString("header", "false")
    delimiter = componentProps.getString("delimiter", ",")
    quote = componentProps.getString("quote", "\"")
    escape = componentProps.getString("escape", "\\")
    charset = componentProps.getString("charset", "UTF-8")
    nullValue = componentProps.getString("nullValue")
    dateFormat = componentProps.getString("dateFormat")
    codec = componentProps.getString("codec")
    quoteMode = componentProps.getString("quoteMode", "NONE")

    StringUtils.assertIsBlank(path)
  }

  override def apply(inputT: SparkData): Unit = {
    inputT.getRawData.repartition(numPartitions).write.format("com.databricks.spark.csv")
        .option("header", header)
        .option("delimiter", delimiter)
        .option("quote", quote)
        .option("escape", escape)
        .option("charset", charset)
        .option("nullValue", nullValue)
        .option("dateFormat", dateFormat)
        .option("codec", codec)
        .option("quoteMode", quoteMode).save(path)

  }
}
