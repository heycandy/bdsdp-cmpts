package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.apache.commons.lang.StringUtils
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.slf4j.Logger

/**
 * Created by water on 2017.6.29.
 */
class SVMPredict (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var path: String = null

  override def apply(inputT: SparkData): SparkData = {
    val df = inputT.getRawData
    val sc = df.sqlContext.sparkContext
    val sameModel = SVMModel.load(sc, path)
    val data: RDD[Vector] = df.mapPartitions(
      iterator => iterator.map(row => {
        val values = row.toSeq.map(_.toString).map(_.toDouble).toArray
        Vectors.dense(values)
      }))
    val rddPredict: RDD[Double] = sameModel.predict(data)
    val rddRow: RDD[Row] = rddPredict.mapPartitions(
      iterator => iterator.map(row => {
        Row(row)
      })
    )
    //rdd转换为dataframe
    val structType = StructType(Array(
      StructField("label", DoubleType, true)
    ))
    val dfResult = df.toDF().sqlContext.createDataFrame(rddRow, structType)
    Builder.build(dfResult)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
  }
}