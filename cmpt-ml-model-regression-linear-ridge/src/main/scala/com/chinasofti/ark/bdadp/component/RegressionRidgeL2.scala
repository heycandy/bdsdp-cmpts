package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LassoWithSGD, RidgeRegressionWithSGD}
import org.slf4j.Logger

/**
  * Created by Hu on 2017/6/22.
  */
class RegressionRidgeL2  (id: String, name: String, log: Logger)
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
    val model = RidgeRegressionWithSGD.train(training,numIterations,stepSize,miniBatchFraction)

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
}
