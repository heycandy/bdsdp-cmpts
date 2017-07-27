package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
/*import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}*/
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.slf4j.Logger

/**
  * Created by Hu on 2017/6/28.
  */
class PredictClusteringKmeans (id: String, name: String, log: Logger)
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
    val sameModel = KMeansModel.load(path)
    info("---- K-means Cluster result: ")
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

/*class PredictClusteringKmeans (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var path: String = null

  override def apply(inputT: SparkData): SparkData = {
    val sc = inputT.getRawData.sqlContext.sparkContext
    val sameModel = KMeansModel.load(sc, path)
    val data: RDD[Vector] = inputT.getRawData.mapPartitions(
      iterator => iterator.map(row => {
        val values = row.toSeq.map(_.toString).map(_.toDouble).toArray
        Vectors.dense(values)
      }))
    val rddPredict: RDD[Int] = sameModel.predict(data)
    val rddRow: RDD[Row] = rddPredict.mapPartitions(
      iterator => iterator.map(row => {
        Row(row)
      })
    )
    //rdd转换为dataframe
    val structType = StructType(Array(
      StructField("Type", IntegerType, true)
    ))
    val dfResult = inputT.getRawData.toDF().sqlContext.createDataFrame(rddRow, structType)
    Builder.build(dfResult)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
  }
}*/












