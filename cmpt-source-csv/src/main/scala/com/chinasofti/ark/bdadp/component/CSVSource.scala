package com.chinasofti.ark.bdadp.component

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

import scala.collection.JavaConversions._

/**
 * Created by White on 2017/1/17.
 */
class CSVSource(id: String, name: String, log: Logger)
    extends SourceComponent[StringData](id, name, log) with Configureable with
            SparkSourceAdapter[SparkData] {

  var path: String = null
  var header: String = null
  var delimiter: String = null
  var quote: String = null
  var escape: String = null
  var parserLib: String = null
  var charset: String = null
  var inferSchema: String = null
  var comment: String = null
  var nullValue: String = null
  var mode: String = null
  var dateFormat: String = null

  override def call(): StringData = {
    Builder.build(
      ("" +: Files.readAllLines(Paths.get(path.replace("file:///", "")), Charset.forName(charset)))
          .mkString("\n"))
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    header = componentProps.getString("header", "false")
    delimiter = componentProps.getString("delimiter", ",")
    quote = componentProps.getString("quote", "\"")
    escape = componentProps.getString("escape", "\\")
    parserLib = componentProps.getString("parserLib", "commons")
    mode = componentProps.getString("mode", "PERMISSIVE")
    charset = componentProps.getString("charset", "UTF-8")
    inferSchema = componentProps.getString("inferSchema", "false")
    comment = componentProps.getString("comment", "#")
    nullValue = componentProps.getString("nullValue")
    dateFormat = componentProps.getString("dateFormat")

    StringUtils.assertIsBlank(path);
  }

  override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {
    Builder.build(sparkScenarioOptions.sqlContext().read
                      .format("com.databricks.spark.csv")
                      .option("path", path)
                      .option("header", header)
                      .option("delimiter", delimiter)
                      .option("quote", quote)
                      .option("escape", escape)
                      .option("parserLib", parserLib)
                      .option("mode", mode)
                      .option("charset", charset)
                      .option("inferSchema", inferSchema)
                      .option("comment", comment)
                      .option("nullValue", nullValue)
                      .option("dateFormat", dateFormat)
                      .load())
  }
}
