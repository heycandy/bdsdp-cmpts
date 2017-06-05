package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.channel.MemoryChannel
import com.chinasofti.ark.bdadp.component.api.data.Data
import com.chinasofti.ark.bdadp.component.api.options.{PipelineOptionsFactory, ScenarioOptions}
import com.chinasofti.ark.bdadp.component.api.sink.SinkComponent
import com.chinasofti.ark.bdadp.component.api.source.SourceComponent
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.component.support.{SinkTask, SourceTask, TransformableTask}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
<<<<<<< HEAD
 * Created by White on 2017/1/3.
 */
=======
  * Created by White on 2017/1/3.
  */
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
object App3 {

  def main(args: Array[String]) {
    val options = PipelineOptionsFactory.fromArgs(args).as(classOf[ScenarioOptions])

    options.setDebug(true)
    options.setScenarioId("1")
    options.setExecutionId("1")

    val source = options.getSettings.getOrDefault("pipeline.source",
      """{"id": "1", "name": "CSVSource", "path": "D:\\csv.csv", "header": "true"}""")
    val transform = options.getSettings.getOrDefault("pipeline.transform",
      """{"id": "2", "name": "Filter", "conditionExpr": "dac>100"}""")
    val sink = options.getSettings.getOrDefault("pipeline.sink",
      """{"id": "3", "name": "LoggerSink", "numRows": "20"}""")

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    val sourceModel = mapper.readValue[SourceModel](source)
    val transformModel = mapper.readValue[TransformModel](transform)
    val sinkModel = mapper.readValue[SinkModel](sink)

    val sourceClazz = Class.forName("com.chinasofti.ark.bdadp.component.CSVSource")
      .asInstanceOf[Class[SourceComponent[_ <: Data[_]]]]
    val sourceTask = new SourceTask(sourceModel.id, sourceModel.name, options, sourceClazz)

    val transformClazz = Class.forName("com.chinasofti.ark.bdadp.component.Filter")
      .asInstanceOf[Class[TransformableComponent[_ <: Data[_], _ <: Data[_]]]]
    val transformTask = new
        TransformableTask(transformModel.id, transformModel.name, options, transformClazz)

    val sinkClazz = Class.forName("com.chinasofti.ark.bdadp.component.LoggerSink")
      .asInstanceOf[Class[SinkComponent[_ <: Data[_]]]]
    val sinkTask = new SinkTask(sinkModel.id, sinkModel.name, options, sinkClazz)

    val channel1 = new MemoryChannel
    val channel2 = new MemoryChannel

    val props = new ComponentProps

    props.setProperty("path", sourceModel.path)
    props.setProperty("header", sourceModel.header)
    props.setProperty("conditionExpr", transformModel.conditionExpr)
    props.setProperty("numRows", sinkModel.numRows)

    sourceTask.addOChannel(channel1)
    transformTask.addIChannel(channel1)
    transformTask.addOChannel(channel2)
    sinkTask.addIChannel(channel2)

    sourceTask.configure(props)
    transformTask.configure(props)
    sinkTask.configure(props)

    sourceTask.run()
    transformTask.run()
    sinkTask.run()

  }

  case class SourceModel(id: String, name: String, path: String, header: String)

  case class TransformModel(id: String, name: String, conditionExpr: String)

  case class SinkModel(id: String, name: String, numRows: String)

}
