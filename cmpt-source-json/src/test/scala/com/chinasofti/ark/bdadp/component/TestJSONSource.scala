package com.chinasofti.ark.bdadp.component

import java.util

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.options.{SparkScenarioOptions, ScenarioOptions, PipelineOptionsFactory}
import com.chinasofti.ark.bdadp.component.api.transforms.MultiTransComponent
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.slf4j.LoggerFactory

/**
 * Created by Administrator on 2017.2.8.
 */
object TestJSONSource {

  def main(args: Array[String]) {
    val log = LoggerFactory.getLogger(this.getClass)

    val options = PipelineOptionsFactory.fromArgs(args).as(classOf[ScenarioOptions])

    val transform = options.getParameter.getOrDefault("pipeline.transform",
      """[{"id": "1", "name": "jSONSource", "path": "D:\aaa.json" , "charset": "UTF-8"}]""")

    options.setDebug(true)

//    val json1 = options.as(classOf[SparkScenarioOptions]).sparkContext().parallelize(input :: Nil)
//    val rawData1 = options.as(classOf[SparkScenarioOptions]).sqlContext().jsonRDD(json1)
//    val data1 = new SparkData(rawData1)

    val inputT = new java.util.ArrayList[SparkData]()

//    inputT.add(data1)

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    val pipeline = mapper.readValue[Seq[SourceModel]](transform).map(f => {
      val className = Array("com.chinasofti.ark.bdadp.component",
        f.name.charAt(0).toUpper + f.name.substring(1)).mkString(".")
      val clazz = Class.forName(className)

      val constructor = clazz.getConstructors()(0)
      val obj = constructor.newInstance(f.id, f.name, log)

      val props = new ComponentProps()

      props.setProperty("path", f.path)
      props.setProperty("charset", f.charset)

      obj.asInstanceOf[Configureable].configure(props)
      obj.asInstanceOf[MultiTransComponent[util.Collection[SparkData], SparkData]]

    }).map(_.apply(inputT))


    pipeline.head.getRawData.show()

  }

  case class SourceModel(id: String, name: String, path: String,charset: String)

}
