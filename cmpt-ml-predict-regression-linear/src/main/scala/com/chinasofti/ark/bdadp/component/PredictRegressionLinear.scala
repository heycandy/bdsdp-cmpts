package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.slf4j.Logger

/**
  * Created by Hu on 2017/6/21.
  */
class PredictRegressionLinear (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var path: String = null
  var features:Array[String] =null

  override def apply(inputT: SparkData): SparkData = {
    val sc = inputT.getRawData.sqlContext.sparkContext

    val df = inputT.getRawData
    info("------------data input:")
    df.repartition(8).take(10).foreach(row => info(row.toString()))
    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")

    val output = assembler.transform(df)
    val sameModel = LinearRegressionModel.load(path)
    info("---- Linear regression pridict results: ")
    val transformed = sameModel.transform(output)
    info(sameModel.toString()+": " + sameModel.getPredictionCol)
    transformed.select("features",sameModel.getPredictionCol).repartition(8).take(10).foreach(row => info(row.toString()))
    Builder.build(transformed)

  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    features = componentProps.getString("features","C0").split(",")
  }
}



/*
class PredictRegressionLinear (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var path: String = null

  override def apply(inputT: SparkData): SparkData = {
    val sc = inputT.getRawData.sqlContext.sparkContext
    val sameModel = LinearRegressionModel.load(sc, path)
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
    val structType = StructType(Array(
      StructField("label", DoubleType, true)
    ))
    val dfResult = inputT.getRawData.toDF().sqlContext.createDataFrame(rddRow, structType)
    Builder.build(dfResult)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
  }
}*/
