package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
<<<<<<< HEAD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.Logger

/**
 * Created by Administrator on 2017/1/12.
 */
=======
import org.slf4j.Logger

/**
  * Created by Administrator on 2017/1/12.
  */
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
class PredictModel(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var path: String = null
<<<<<<< HEAD
  override def apply(inputT: SparkData): SparkData = {
    val sc = inputT.getRawData.sqlContext.sparkContext
    val sameModel = DecisionTreeModel.load(sc,path)
=======

  override def apply(inputT: SparkData): SparkData = {
    val sc = inputT.getRawData.sqlContext.sparkContext
    val sameModel = DecisionTreeModel.load(sc, path)
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
    val data: RDD[Vector] = inputT.getRawData.mapPartitions(
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
<<<<<<< HEAD
    val structType=StructType(Array(
      StructField("label",DoubleType,true)
    ))
    val dfResult = inputT.getRawData.toDF().sqlContext.createDataFrame(rddRow,structType)
=======
    val structType = StructType(Array(
      StructField("label", DoubleType, true)
    ))
    val dfResult = inputT.getRawData.toDF().sqlContext.createDataFrame(rddRow, structType)
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
    Builder.build(dfResult)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
  }
}
