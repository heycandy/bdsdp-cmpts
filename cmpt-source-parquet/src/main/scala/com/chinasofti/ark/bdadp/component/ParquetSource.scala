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
class ParquetSource(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
    SparkSourceAdapter[SparkData] {

  var path: String = null
  var charset: String = null
  var mergeSchema: String = null

  override def call(): StringData = {
    Builder.build(
      ("" +: Files.readAllLines(Paths.get(path.replace("file:///", "")), Charset.forName(charset)))
        .mkString("\n"))
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    charset = componentProps.getString("charset", "UTF-8")
    mergeSchema = componentProps.getString("mergeSchema", "true")
    StringUtils.assertIsBlank(path);
  }

  override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {
    Builder.build(sparkScenarioOptions.sqlContext().read.option("mergeSchema", mergeSchema).parquet(path))
  }
}
