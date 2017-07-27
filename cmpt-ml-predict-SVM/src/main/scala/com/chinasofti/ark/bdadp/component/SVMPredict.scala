package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{StringData, SparkData}
import com.chinasofti.ark.bdadp.component.api.sink.{SparkSinkAdapter, SinkComponent}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg.Vectors
import org.slf4j.Logger

/**
 * Created by water on 2017.6.29.
 */
class SVMPredict(id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null
  var featuresCol: String = null

  override def apply(inputT: StringData): Unit = {
  }

  override def apply(inputT: SparkData): Unit = {
    var df = inputT.getRawData
    var featuresColArr: Array[String] = null
    val sampleModel = SVMModel.load(df.sqlContext.sparkContext, path)
    if (null != featuresCol && !"".equals(featuresCol)) {
      featuresColArr = featuresCol.split(",")
      df = df.selectExpr(featuresColArr: _*)
    }
    val featuresData = df.mapPartitions(
      iterator => iterator.map(row => {
        val values = row.toSeq.map(_.toString).map(_.toDouble).toArray
        Vectors.dense(values)
      }))

    val predictRDD = sampleModel.predict(featuresData)
    val resultRDD = featuresData.zip(predictRDD)
    info("====== predict data and result is ======")
    info("[ " + df.columns.mkString(",") + " ] label" )
    resultRDD.repartition(8).collect().foreach(row => info(row.toString()))
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    featuresCol = componentProps.getString("featuresCol")
  }
}
