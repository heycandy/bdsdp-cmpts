package com.chinasofti.ark.bdadp.component

import java.util.Random

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.util.random.SamplingUtils
import org.slf4j.Logger
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2017/1/12.
 */
class PredictModel(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var path: String = null
  override def apply(inputT: SparkData): SparkData = {
    val sc = inputT.getRawData.sqlContext.sparkContext
    val sameModel = DecisionTreeModel.load(sc,path)
    val data: RDD[Vector] = inputT.getRawData.mapPartitions(
      iterator => iterator.map(row => {
        val values = row.toSeq.tail.map(_.toString).map(_.toDouble).toArray
        Vectors.dense(values)
      }))
    val rddPredict: RDD[Double] = sameModel.predict(data)
    val rddRow: RDD[Row] = rddPredict.mapPartitions(
      iterator => iterator.map(row => {
        Row(row)
      })
    )
    //rdd转换为dataframe
    val structType=StructType(Array(
      StructField("label",DoubleType,true)
    ))
    val dfResult = inputT.getRawData.toDF().sqlContext.createDataFrame(rddRow,structType)
    Builder.build(dfResult)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
  }
}
