package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.slf4j.Logger

/**
  * Created by Hu on 2017/6/19.
  */
class RegressionDecisionTree (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
    SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null
  var trainDataPer: Double = 0.0
  var impurity: String = null
  var maxDepth: Int = 0
  var maxBins: Int = 0
  var labelCol: String = null
  var featuresCol: Array[String] = null

  override def configure(componentProps: ComponentProps):Unit = {
    path = componentProps.getString("path")
    trainDataPer = componentProps.getString("trainDataPer", "0.7").toDouble
    impurity = componentProps.getString("impurity", "variance")   //"variance"  regression
    maxDepth = componentProps.getString("maxDepth", "5").toInt
    maxBins = componentProps.getString("maxBins", "32").toInt
    labelCol = componentProps.getString("labelCol")
    featuresCol = componentProps.getString("featuresCol").split(",")
  }

  override def apply(inputT: StringData): Unit = {

  }

  override def apply(inputT: SparkData): Unit = {

    val df = inputT.getRawData

    val labelDF = df.select(labelCol)
    val featuresDF = df.selectExpr(featuresCol: _*)
    val parsedData = labelDF.join(featuresDF).mapPartitions(iterator => iterator.map(row => {
      val label = row.toSeq.head.toString.toDouble
      val values = row.toSeq.tail.map(_.toString).map(_.toDouble).toArray
      LabeledPoint(label, Vectors.dense(values))
    }))

    val splits = parsedData.randomSplit(Array(trainDataPer, 1.0 - trainDataPer))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
      maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case (v, p) => math.pow(v - p, 2) }.mean()
    info("MSE = " + testMSE)
    // Save and load model
    val sc = inputT.getRawData.sqlContext.sparkContext
    model.save(sc, path)
  }
}






/*
class RegressionDecisionTree (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
    SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null
  var impurity: String = null
  var maxDepth: Int = 0
  var maxBins: Int = 0

  override def configure(componentProps: ComponentProps):Unit = {
    path = componentProps.getString("path")
    impurity = componentProps.getString("impurity", "variance")   //"variance"  regression
    maxDepth = componentProps.getString("maxDepth", "5").toInt
    maxBins = componentProps.getString("maxBins", "32").toInt
  }

  override def apply(inputT: StringData): Unit = {

  }

  override def apply(inputT: SparkData): Unit = {
    val data = inputT.getRawData.mapPartitions(iterator => iterator.map(row => {
      val label = row.toSeq.head.toString.toDouble
      // println("rowLabel: " + row.toSeq.toVector)
      val values = row.toSeq.tail.map(_.toString).map(_.toDouble).toArray
      val features = Vectors.dense(values)
      new LabeledPoint(label, features)
    }))

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
      maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case (v, p) => math.pow(v - p, 2) }.mean()
    info("MSE = " + testMSE)
    // Save and load model
    val sc = inputT.getRawData.sqlContext.sparkContext
    model.save(sc, path)
  }
}*/



