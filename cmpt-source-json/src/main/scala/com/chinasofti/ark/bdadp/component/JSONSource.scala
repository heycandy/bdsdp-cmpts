package com.chinasofti.ark.bdadp.component

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger

import scala.collection.JavaConversions._
/**
 * Created by Administrator on 2017.2.8.
 */
class JSONSource(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
  SparkSourceAdapter[SparkData] {

  var path: String = null
  var charset: String = null

  override def call(): StringData = {
    val newPath = ("" +: Files.readAllLines(Paths.get(path.replace("file:///", "")), Charset.forName(charset)))
      .mkString("\n")
    Builder.build(newPath)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    charset = componentProps.getString("charset", "UTF-8")
  }

  override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {
    val sqlContext = sparkScenarioOptions.sqlContext()
    val jsonDF = sqlContext.read.json(path)
    Builder.build(jsonDF)
  }
}
