package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.channel.MemoryChannel
import com.chinasofti.ark.bdadp.component.api.data.{Data, SparkData}
import com.chinasofti.ark.bdadp.component.api.options.{PipelineOptionsFactory, ScenarioOptions, SparkScenarioOptions}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.component.support.TransformableTask
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * Created by White on 2017/1/3.
 */

object App {

  def main(args: Array[String]) {

    //实例化一个 PipelineOptions
    val options = PipelineOptionsFactory.fromArgs(args).as(classOf[ScenarioOptions])

    val input = options.getSettings.getOrDefault("pipeline.input",
                                                  """[{"name": "xiao", "age": 25},{"name": "xiao", "age": 25},{"name": "bai", "age": 30},{"name": "bai", "age": 30},{"name": "bai", "age": 30},{"name": "hh", "age": 15}]""")
    val transform = options.getSettings.getOrDefault("pipeline.transform",
                                                      """[{"id": "1", "name": "distinct","withReplacement":"true","fraction":"0.7"}]""")

    options.setDebug(true)
    options.setScenarioId("1")
    options.setExecutionId("1")

    //制作RDD，Nil是一个空的List[Nothing]

    //:: 该方法被称为cons，意为构造，向队列的头部追加数据，创造新的列表。用法为 x::list,其中x为加入到头部的元素，
    // 无论x是列表与否，它都只将成为新生成列表的第一个元素，也就是说新生成的列表长度为list的长度＋1(btw, x::list等价于list.::(x))
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
                            f.name.head.toUpper + f.name.tail).mkString(".")
      val clazz = Class.forName(className)
          .asInstanceOf[Class[TransformableComponent[_ <: Data[_], _ <: Data[_]]]]

      val task = new TransformableTask(f.id, f.name, options, clazz)
      val props = new ComponentProps()

      props.setProperty("withReplacement", f.withReplacement.toString)
      props.setProperty("fraction", f.fraction.toString)
      //      props.setProperty("seed", f.seed.toString)


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

  }

  case class TransformModel(id: String, name: String, withReplacement: Boolean, fraction: Double)

}
