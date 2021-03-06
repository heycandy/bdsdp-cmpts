package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StreamData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.{ScenarioOptions, SparkScenarioOptions}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter, StreamSinkAdapter}
import com.chinasofti.ark.bdadp.component.api.{Configureable, Optional}
import org.slf4j.Logger

/**
  * Hello world!
  *
  */
class LoggerSink(id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log)
    with Configureable with Optional with Serializable
    with SparkSinkAdapter[SparkData]
    with StreamSinkAdapter[StreamData] with Serializable {

  var numRows: Int = _
  var numPartitions: Int = _

  var options: ScenarioOptions = _

  override def configure(componentProps: ComponentProps): Unit = {
    numRows = componentProps.getInt("numRows", 20)
    numPartitions = componentProps.getInt("numPartitions", 1)
  }

  override def options(
    scenarioOptions: ScenarioOptions): Unit = options = scenarioOptions

  override def apply(inputT: StringData): Unit = {
    info(inputT.getRawData)
  }

  override def apply(inputT: SparkData): Unit = {
    val firstNum =
      inputT.getRawData.repartition(numPartitions).take(numRows + 1)
    info(inputT.getRawData.toString())
    firstNum.foreach(row => info(row.toString()))
    if (firstNum.length > numRows) {
      info("...")
    }
  }

  override def apply(inputT: StreamData): Unit = {
    inputT.getRawData.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val df = options.as(classOf[SparkScenarioOptions])
          .sqlContext().read.json(rdd)
        val firstNum = df.repartition(numPartitions).take(numRows + 1)
        info(df.toString())
        firstNum.foreach(row => info(row.toString()))
        info("-------------------------------------------")
        info("")
      }
    })
  }


}
