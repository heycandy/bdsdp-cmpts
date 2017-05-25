package com.chinasofti.ark.bdadp.component

import java.util.Date

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
 * Created by Administrator on 2017.2.10.
 */
=======
  * Created by Administrator on 2017.2.10.
  */
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
object TestJDBC {

  def main(args: Array[String]) {
    val options = PipelineOptionsFactory.fromArgs(args).as(classOf[ScenarioOptions])

    options.setDebug(true)
    options.setScenarioId("1")
    options.setExecutionId("1")
    val now = new Date()
    val conditionExpr = "NAME = 'newName'"
<<<<<<< HEAD
    testSelect(options,conditionExpr)
    val end = new Date()

    println(end.getTime-now.getTime)
=======
    testSelect(options, conditionExpr)
    val end = new Date()

    println(end.getTime - now.getTime)
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

  }


<<<<<<< HEAD
  def testSelect(options: ScenarioOptions,conditionExpr: String):Unit = {
=======
  def testSelect(options: ScenarioOptions, conditionExpr: String): Unit = {
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

    val source = options.getSettings.getOrDefault("pipeline.source",
      """{"id": "1", "name": "JDBCSource", "conUrl": "jdbc:mysql://localhost:3306/testjdbc?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true", "table": "example", "userName": "root", "passWord": "mysql"}""")
    val source2 = options.getSettings.getOrDefault("pipeline.source",
      """{"id": "1", "name": "JDBCSource", "conUrl": "jdbc:mysql://localhost:3306/testjdbc2?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true", "table": "example", "userName": "root", "passWord": "mysql"}""")
    //    val source3 = options.getSettings.getOrDefault("pipeline.source",
<<<<<<< HEAD
//      """{"id": "1", "name": "JDBCSource", "conUrl": "jdbc:mysql://localhost:3306/testjdbc3?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true", "table": "example", "userName": "root", "passWord": "mysql"}""")
=======
    //      """{"id": "1", "name": "JDBCSource", "conUrl": "jdbc:mysql://localhost:3306/testjdbc3?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true", "table": "example", "userName": "root", "passWord": "mysql"}""")
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
    val transform = options.getSettings.getOrDefault("pipeline.transform",
      """{"id": "2", "name": "Filter", "conditionExpr": "example_id = 11111"}""")
    val sink = options.getSettings.getOrDefault("pipeline.sink",
      """{"id": "3", "name": "LoggerSink", "numRows": "1"}""")

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    val sourceModel = mapper.readValue[SourceModel](source)
    val sourceModel2 = mapper.readValue[SourceModel](source2)
<<<<<<< HEAD
//    val sourceModel3 = mapper.readValue[SourceModel](source3)
=======
    //    val sourceModel3 = mapper.readValue[SourceModel](source3)
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
    val transformModel = mapper.readValue[TransformModel](transform)
    val sinkModel = mapper.readValue[SinkModel](sink)

    val sourceClazz = Class.forName("com.chinasofti.ark.bdadp.component.JDBCSource")
      .asInstanceOf[Class[SourceComponent[_ <: Data[_]]]]
    val sourceTask = new SourceTask(sourceModel.id, sourceModel.name, options, sourceClazz)
    val sourceTask2 = new SourceTask(sourceModel2.id, sourceModel2.name, options, sourceClazz)
<<<<<<< HEAD
//    val sourceTask3 = new SourceTask(sourceModel3.id, sourceModel3.name, options, sourceClazz)
=======
    //    val sourceTask3 = new SourceTask(sourceModel3.id, sourceModel3.name, options, sourceClazz)
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

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

    props.setProperty("conUrl", sourceModel.conUrl)
    props.setProperty("table", sourceModel.table)
    props.setProperty("userName", sourceModel.userName)
    props.setProperty("passWord", sourceModel.passWord)

    props.setProperty("conditionExpr", conditionExpr)
    props.setProperty("numRows", sinkModel.numRows)

    sourceTask.addOChannel(channel1)
    sourceTask2.addOChannel(channel1)
<<<<<<< HEAD
//    sourceTask3.addOChannel(channel1)
=======
    //    sourceTask3.addOChannel(channel1)
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

    transformTask.addIChannel(channel1)
    transformTask.addOChannel(channel2)
    sinkTask.addIChannel(channel2)

    sourceTask.configure(props)
    sourceTask2.configure(props)
<<<<<<< HEAD
//    sourceTask3.configure(props)
=======
    //    sourceTask3.configure(props)
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
    transformTask.configure(props)
    sinkTask.configure(props)

    sourceTask.run()
    sourceTask2.run()
<<<<<<< HEAD
//    sourceTask3.run()
=======
    //    sourceTask3.run()
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
    transformTask.run()
    sinkTask.run()
  }

  case class SourceModel(id: String, name: String, conUrl: String, table: String, userName: String, passWord: String)

  case class TransformModel(id: String, name: String, conditionExpr: String)

  case class SinkModel(id: String, name: String, numRows: String)

}

