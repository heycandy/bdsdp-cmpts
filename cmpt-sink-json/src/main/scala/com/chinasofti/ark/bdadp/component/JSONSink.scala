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
class JSONSink(id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log)
    with Configureable with Optional with Serializable
    with SparkSinkAdapter[SparkData]
    with StreamSinkAdapter[StreamData] {

  var path: String = _
  var saveMode: String = _
  var numPartitions: Int = 1

  var options: ScenarioOptions = _

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    saveMode = componentProps.getString("saveMode", "error")
    numPartitions = componentProps.getInt("numPartitions", 1)
  }

  override def options(
    scenarioOptions: ScenarioOptions): Unit = options = scenarioOptions


  override def apply(inputT: StringData): Unit = {
    info(inputT.getRawData)
  }

  override def apply(inputT: SparkData): Unit = {
    inputT.getRawData.repartition(numPartitions).write.json(path)
  }

  override def apply(inputT: StreamData): Unit = {
    inputT.getRawData.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        options.as(classOf[SparkScenarioOptions])
          .sqlContext().read.json(rdd)
          .repartition(numPartitions)
          .write.mode(saveMode)
          .json(path)
      }
    })
  }


}
