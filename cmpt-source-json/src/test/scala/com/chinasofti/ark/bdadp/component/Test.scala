package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.channel.MemoryChannel
import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.options.{SparkScenarioOptions, ScenarioOptions, PipelineOptionsFactory}
import com.chinasofti.ark.bdadp.component.support.TransformableTask
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.slf4j.LoggerFactory

/**
 * Created by Administrator on 2017.2.9.
 */
object Test {

  def main(args: Array[String]) {
    val log = LoggerFactory.getLogger(this.getClass)
    val json = new JSONSource("1","json",log)
    json.path="D:\\aaa.json"


//    val json = options.as(classOf[SparkScenarioOptions]).sparkContext().parallelize(input :: Nil)
    val options = PipelineOptionsFactory.fromArgs(args).as(classOf[ScenarioOptions])
//    val rawData = options.as(classOf[SparkScenarioOptions]).sqlContext().jsonRDD(json)
//    val data = new SparkData(rawData)
    val source = new MemoryChannel
    val sink = new MemoryChannel

    source.input(json)

  }

}
