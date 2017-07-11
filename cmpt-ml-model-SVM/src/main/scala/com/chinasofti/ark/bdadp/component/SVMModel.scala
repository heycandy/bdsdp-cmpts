package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SparkSinkAdapter, SinkComponent}
import org.apache.commons.lang.StringUtils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger

/**
 * Created by water on 2017.6.28.
 */
class SVMModel(id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null
  var trainDataPer: Double = 0.0
  var labelCol: String = null
  var featuresCol: Array[String] = null
  var numIterations: Int = 0
  var stepSize: Double = 0.0
  var regParam: Double = 0
  var miniBatchFraction: Double = 0

  override def apply(inputT: StringData): Unit = {
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    trainDataPer = componentProps.getString("trainDataPer", "0.7").toDouble
    labelCol = componentProps.getString("labelCol")
    featuresCol = componentProps.getString("featuresCol").split(",")
    numIterations = componentProps.getString("numIterations", "100").toInt
    stepSize = componentProps.getString("stepSize", "1.0").toDouble
    regParam = componentProps.getString("regParam", "0.01").toDouble
    miniBatchFraction = componentProps.getString("miniBatchFraction", "1.0").toDouble
  }

  override def apply(inputT: SparkData): Unit = {
    val df = inputT.getRawData
    printInput(df)

    val labelDF = df.select(labelCol)
    val featuresDF = df.selectExpr(featuresCol: _*)
    val parsedData = labelDF.join(featuresDF).mapPartitions(iterator => iterator.map(row => {
      val label = row.toSeq.head.toString.toDouble
      val values = row.toSeq.tail.map(_.toString).map(_.toDouble).toArray
      LabeledPoint(label, Vectors.dense(values))
    }))

    val splits = parsedData.randomSplit(Array(trainDataPer, 1.0 - trainDataPer))
    val (trainingData, testData) = (splits(0), splits(1))


    val model = SVMWithSGD.train(trainingData, numIterations, stepSize, regParam, miniBatchFraction)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testAccuracy = labelAndPreds.filter(r =>
      r._1 == r._2).count.toDouble / testData.count

    info("Test Accuracy = " + testAccuracy)
    val sc = df.sqlContext.sparkContext
    model.save(sc, path)

  }

  def printInput(df: DataFrame): Unit = {
    ("" :: df.toString() ::
      Nil ++ df.repartition(8).take(10)).foreach(row => info(row.toString()))
  }
}
