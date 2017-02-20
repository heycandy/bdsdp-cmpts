package com.chinasofti.ark

import com.chinasofti.ark.bdadp.component.ComponentProps
import com.chinasofti.ark.bdadp.component.api.channel.MemoryChannel
import com.chinasofti.ark.bdadp.component.api.data.{Data, SparkData}
import com.chinasofti.ark.bdadp.component.api.options.{PipelineOptionsFactory, ScenarioOptions, SparkScenarioOptions}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.component.support.TransformableTask
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.slf4j.LoggerFactory

/**
 * Created by Administrator on 2017.2.9.
 */
object TestFill {

  def main(args: Array[String]) {

    val log = LoggerFactory.getLogger(this.getClass)
    val options = PipelineOptionsFactory.fromArgs(args).as(classOf[ScenarioOptions])

    val input = options.getSettings.getOrDefault("pipeline.input",
      """[{"name": "aaa", "age": null, "money": null},{"name": "bbb", "age": 25, "money": null},{"name": "ccc", "age": 30, "money": null}]""")

    val transform = options.getSettings.getOrDefault("pipeline.transform",
      """[{"id": "1", "name": "fill","value":"100","cols":"age,money","valueType":"double","delimiter":","}]""")

    options.setDebug(true)

    val json = options.as(classOf[SparkScenarioOptions]).sparkContext().parallelize(input :: Nil)
    val rawData = options.as(classOf[SparkScenarioOptions]).sqlContext().jsonRDD(json)
    val data = new SparkData(rawData)
    val source = new MemoryChannel
    val sink = new MemoryChannel

    source.input(data)

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    val pipeline = mapper.readValue[Seq[TransformModel]](transform).map(f => {
      val className = Array("com.chinasofti.ark.bdadp.component",
        f.name.charAt(0).toUpper + f.name.substring(1)).mkString(".")
      val clazz = Class.forName(className)
        .asInstanceOf[Class[TransformableComponent[_ <: Data[_], _ <: Data[_]]]]

      val task = new TransformableTask(f.id, f.name, options, clazz)
      val props = new ComponentProps()

      props.setProperty("value", f.value)
      props.setProperty("cols", f.cols)
      props.setProperty("valueType", f.valueType)
      props.setProperty("delimiter", f.delimiter)

      task.configure(props)

      task

    })

    pipeline.head.addIChannel(source)
    pipeline.last.addOChannel(sink)

    val that = pipeline.tail

    pipeline.zip(that).foreach {
      case (out, in) =>
        val channel = new MemoryChannel

        out.addOChannel(channel)
        in.addIChannel(channel)
    }

    pipeline.foreach(_.run())

    sink.output().asInstanceOf[SparkData].getRawData.collect().foreach(println)
    sink.output().asInstanceOf[SparkData].getRawData.columns.foreach(println)

  }

  case class TransformModel(id: String, name: String, value: String, cols: String, valueType: String, delimiter: String)

}


