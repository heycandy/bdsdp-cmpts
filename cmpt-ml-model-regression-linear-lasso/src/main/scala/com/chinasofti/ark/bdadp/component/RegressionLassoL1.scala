package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LassoWithSGD, LinearRegressionWithSGD}
import org.slf4j.Logger

/**
  * Created by Hu on 2017/6/22.
  */
class RegressionLassoL1  (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
    SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null

  var trainDataSplit:Double = 0.0
 // var testDataSplit:Double = 0.0
  var numIterations:Int = 0
  var stepSize:Double = 0
  var miniBatchFraction:Double = 1.0
  var labelCol: String = null
  var featuresCol: Array[String] = null

  override def configure(componentProps: ComponentProps):Unit = {
    path = componentProps.getString("path")

    trainDataSplit = componentProps.getString("trainDataSplit", "0.8").toDouble
   // testDataSplit = componentProps.getString("testDataSplit", "0.2").toDouble
    numIterations = componentProps.getString("numIterations","100").toInt
    stepSize = componentProps.getString("stepSize", "1").toDouble
    miniBatchFraction = componentProps.getString("miniBatchFraction","1.0").toDouble
    labelCol = componentProps.getString("labelCol")
    featuresCol = componentProps.getString("featuresCol").split(",")
  }

  override def apply(inputT: StringData): Unit = {

  }

  override def apply(inputT: SparkData): Unit = {

    val df = inputT.getRawData
    val labelDF = df.select(labelCol)
    labelDF.show(20)
    val featuresDF = df.selectExpr(featuresCol: _*)
    featuresDF.show(20)
    val exprArr = labelCol +: featuresCol

    val newDf = df.selectExpr(exprArr:_*)
    println("<<<<<<<<new Df total all:   <<<<<,")
    newDf.show()
      val parsedData = newDf.mapPartitions(iterator => iterator.map(row => {
      val label = row.toSeq.head.toString.toDouble
      val values = row.toSeq.tail.map(_.toString).map(_.toDouble).toArray
      LabeledPoint(label, Vectors.dense(values))
    })).cache()

    val splits = parsedData.randomSplit(Array(trainDataSplit,1 - trainDataSplit))
    val (trainingData, testData) = (splits(0), splits(1))



    info("-------Number of Training: " + trainingData.count())
    trainingData.collect() foreach println
    info("-------Number of Testing: " + testData.count())
    testData.collect() foreach println
    val model = LassoWithSGD.train(parsedData,numIterations,stepSize,miniBatchFraction)
    info("----weight: " + model.weights.toString)
    //prediction
    val prediction = model.predict(testData.map(_.features))
    println("<<<<<<<<<<<<<<<<<<<<prediction<<<<<<<<<<<<<<<<")
    println(prediction)
    info("prediction; " + prediction)
    val predictionAndLabel = prediction.zip(testData.map(_.label))
    //compute the loss
    val loss = predictionAndLabel.map({
      case(p,l) =>
        val err = p - l
        err*err
    }).reduce(_ + _)
    info("----loss: " + loss)
    val rmse = math.sqrt(loss / testData.count())
    info("-----RMSE: "  + rmse)

    // Save and load model
    val sc = inputT.getRawData.sqlContext.sparkContext
    model.save(sc, path)

  }
}

/*
class RegressionLassoL1  (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
    SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null

  var trainDataSplit:Double = 0.0
  var testDataSplit:Double = 0.0
  var numIterations:Int = 0
  var stepSize:Double = 0
  var miniBatchFraction:Double = 1.0

  override def configure(componentProps: ComponentProps):Unit = {
    path = componentProps.getString("path")

    trainDataSplit = componentProps.getString("trainDataSplit", "0.8").toDouble
    testDataSplit = componentProps.getString("testDataSplit", "0.2").toDouble
    numIterations = componentProps.getString("numIterations","100").toInt
    stepSize = componentProps.getString("stepSize", "1").toDouble
    miniBatchFraction = componentProps.getString("miniBatchFraction","1.0").toDouble
  }

  override def apply(inputT: StringData): Unit = {

  }

  override def apply(inputT: SparkData): Unit = {

    inputT.getRawData.collect() foreach(a=>println("haha: " + a))
    val data = inputT.getRawData.mapPartitions(iterator => iterator.map(row => {
      val parts = row.getString(0).split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }))

    //train model and split dataset into trainSample and testSample
    val splits = data.randomSplit(Array(trainDataSplit,testDataSplit))
    val training = splits(0).cache()
    val test = splits(1).cache()
    info("-------total of DataSet: " + data.count())
    info("-------Number of Training: " + training.count())
    info("-------Number of Testing: " + test.count())
    val model = LassoWithSGD.train(training,numIterations,stepSize,miniBatchFraction)

    //prediction
    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    //compute the loss
    val loss = predictionAndLabel.map({
      case(p,l) =>
        val err = p - l
        err*err
    }).reduce(_ + _)

    val rmse = math.sqrt(loss / test.count())
    info("-----RMSE: "  + rmse)

    // Save and load model
    val sc = inputT.getRawData.sqlContext.sparkContext
    model.save(sc, path)

  }
}*/
