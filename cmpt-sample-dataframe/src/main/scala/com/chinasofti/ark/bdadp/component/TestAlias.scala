package com.chinasofti.ark.bdadp.component

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
<<<<<<< HEAD
 * Created by Administrator on 2017.1.16.
 */
=======
  * Created by Administrator on 2017.1.16.
  */
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
object TestAlias {

  def main(args: Array[String]) {

    val log = LoggerFactory.getLogger(this.getClass)
    val options = PipelineOptionsFactory.fromArgs(args).as(classOf[ScenarioOptions])

    val input = options.getSettings.getOrDefault("pipeline.input",
<<<<<<< HEAD
                                                  """[{"name": "xiao", "age": 25},{"name": "xiao", "age": 25},{"name": "bai", "age": 30}]""")

    val transform = options.getSettings.getOrDefault("pipeline.transform",
                                                      """[{"id": "1", "name": "alias","existingName":"age","newName":"age2"}]""")
=======
      """[{"name": "xiao", "age": 25},{"name": "xiao", "age": 25},{"name": "bai", "age": 30}]""")

    val transform = options.getSettings.getOrDefault("pipeline.transform",
      """[{"id": "1", "name": "alias","existingName":"age","newName":"age2"}]""")
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

    options.setDebug(true)
    options.setScenarioId("1")
    options.setExecutionId("1")

    val json = options.as(classOf[SparkScenarioOptions]).sparkContext().parallelize(input :: Nil)
    val rawData = options.as(classOf[SparkScenarioOptions]).sqlContext().jsonRDD(json)
    val data = new SparkData(rawData)
    val source = new MemoryChannel
    val sink = new MemoryChannel

    source.input(data)

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    //    log.info("-----------" + Seq[TransformModel])

    val pipeline = mapper.readValue[Seq[TransformModel]](transform).map(f => {
      val className = Array("com.chinasofti.ark.bdadp.component",
<<<<<<< HEAD
                            f.name.charAt(0).toUpper + f.name.substring(1)).mkString(".")
      val clazz = Class.forName(className)
          .asInstanceOf[Class[TransformableComponent[_ <: Data[_], _ <: Data[_]]]]
=======
        f.name.charAt(0).toUpper + f.name.substring(1)).mkString(".")
      val clazz = Class.forName(className)
        .asInstanceOf[Class[TransformableComponent[_ <: Data[_], _ <: Data[_]]]]
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

      val task = new TransformableTask(f.id, f.name, options, clazz)
      val props = new ComponentProps()

      props.setProperty("existingName", f.existingName)
      props.setProperty("newName", f.newName)

      task.configure(props)

      task

    })

    pipeline.head.addIChannel(source)
    pipeline.last.addOChannel(sink)

    val that = pipeline.tail

    pipeline.zip(that).foreach {
<<<<<<< HEAD
                                 case (out, in) =>
                                   val channel = new MemoryChannel

                                   out.addOChannel(channel)
                                   in.addIChannel(channel)
                               }
=======
      case (out, in) =>
        val channel = new MemoryChannel

        out.addOChannel(channel)
        in.addIChannel(channel)
    }
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

    pipeline.foreach(_.run())

    sink.output().asInstanceOf[SparkData].getRawData.collect().foreach(println)
    sink.output().asInstanceOf[SparkData].getRawData.columns.foreach(println)

  }

  case class TransformModel(id: String, name: String, existingName: String, newName: String)

}

