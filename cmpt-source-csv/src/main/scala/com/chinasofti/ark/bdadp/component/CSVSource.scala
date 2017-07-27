package com.chinasofti.ark.bdadp.component

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.apache.spark.sql.types._
import org.slf4j.Logger

import scala.collection.JavaConversions._

/**
  * Created by White on 2017/1/17.
  */
class CSVSource(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
    SparkSourceAdapter[SparkData] {

  var path: String = _
  var header: String = _
  var delimiter: String = _
  var quote: String = _
  var escape: String = _
  var parserLib: String = _
  var charset: String = _
  var inferSchema: String = _
  var comment: String = _
  var nullValue: String = _
  var mode: String = _
  var dateFormat: String = _

  var schemaName: String = _
  var dataType: String = _
  var schema: StructType = _

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

    schemaName = componentProps.getString("schemaName")
    dataType = componentProps.getString("dataType")

    if (schemaName != null) {
      val names = schemaName.split(",")
      val types = dataType.split(",")
      val fields = names.indices.map(i => {
        val dataType = types(i) match {
          case "double" => DoubleType
          case "integer" => IntegerType
          case "string" => StringType
          case _ => StringType
        }

        StructField(names(i), dataType)

      })

      schema = StructType(fields)
    }

    StringUtils.assertIsBlank(path)
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
      .schema(schema)
      .load())
  }
}
