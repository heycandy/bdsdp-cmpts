import com.chinasofti.ark.bdadp.component.ComponentProps
import com.chinasofti.ark.bdadp.component.api.channel.MemoryChannel
import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.options.{PipelineOptionsFactory, ScenarioOptions, SparkScenarioOptions}
import com.chinasofti.ark.bdadp.component.support.TransformableTask
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * Created by Administrator on 2017.1.13.
 */
object TestSort {

  def main(args: Array[String]) {

    val options = PipelineOptionsFactory.fromArgs(args).as(classOf[ScenarioOptions])

    val input = options.getParameter.getOrDefault("pipeline.input",
                                                  """[{"name": "aaa", "age": 34},{"name": "ccc", "age": 25},{"name": "bbb", "age": 30}]""")
    val transform = options.getParameter.getOrDefault("pipeline.transform",
                                                      """[{"id": "1", "name": "sort", "colName": "age" ,"sortExprs": "desc"}]""")

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

      val task = new TransformableTask(f.id, f.name, f.id, f.id, clazz)
      val props = new ComponentProps()

      props.setProperty("colName", f.colName)
      props.setProperty("sortExprs", f.sortExprs)
      task.configure(props)
      task.setOptions(options)

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

  case class TransformModel(id: String, name: String, colName: String, sortExprs: String)

}

