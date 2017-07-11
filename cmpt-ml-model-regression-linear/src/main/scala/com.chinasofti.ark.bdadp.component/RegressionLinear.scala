package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.slf4j.Logger

/**
  * Created by Hu on 2017/6/21.
  */
class RegressionLinear (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
    SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null

  var trainDataSplit:Double = 0.0
  var testDataSplit:Double = 0.0
  var maxIter:Int = 0
  var regParam:Double = 0
  var elasticNetParam:Double = 0.0
  var featureCols:Array[String] =null
  var labelCol:String = null

  override def configure(componentProps: ComponentProps):Unit = {
    path = componentProps.getString("path")

    trainDataSplit = componentProps.getString("trainDataSplit", "0.8").toDouble
    testDataSplit = componentProps.getString("testDataSplit", "0.2").toDouble
    maxIter = componentProps.getString("maxIter","10").toInt
    regParam = componentProps.getString("regParam", "0.3").toDouble
    elasticNetParam = componentProps.getString("elasticNetParam","0.8").toDouble
    featureCols = componentProps.getString("featureCols","C0").split(",")
    labelCol = componentProps.getString("labelCol")
  }

  override def apply(inputT: StringData): Unit = {

  }

  override def apply(inputT: SparkData): Unit = {

  /*  val rowRDD:RDD[Row] = inputT.getRawData.mapPartitions(iterator => iterator.map(row => {
      val nr = row.toSeq.toArray.map(_.toString).map(_.toDouble)

    Row(nr(0),nr(1),nr(2),nr(3),nr(4),nr(5),nr(6))

    }))

    rowRDD.take(10) foreach(println(_))

    val schema = StructType(featureCols.map(fieldName=>StructField(fieldName,DoubleType,true)))*/


  //  val dfResult = inputT.getRawData.sqlContext.createDataFrame(rowRDD,schema)
    val dfResult = inputT.getRawData
    //val df = inputT.getRawData
    info("------------data input:")
    dfResult.take(10).foreach(row => info(row.toString()))
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val splits = dfResult.randomSplit(Array(trainDataSplit,testDataSplit))
    val (trainingData, testData) = (splits(0), splits(1))
    val output = assembler.transform(trainingData)

    val lr = new LinearRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setLabelCol(labelCol)

    //Fit the model
    val lrModel = lr.fit(output)
    // Print the coefficients and intercept for linear regression
    info("Coefficients: " + lrModel.coefficients + " Intercept: " + lrModel.intercept)

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    info("numIterations: " + trainingSummary.totalIterations)
    info("objectiveHistory: " + trainingSummary.objectiveHistory.toList)
    info("-----residuals:")
    trainingSummary.residuals.repartition(8).take(10).foreach(row => info(row.toString()))
    info("RMSE: " + trainingSummary.rootMeanSquaredError)
    info("r2: " + trainingSummary.r2)

    lrModel.save(path)
  }
}

/*class RegressionLinear (id: String, name: String, log: Logger)
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
    val model = LinearRegressionWithSGD.train(training,numIterations,stepSize,miniBatchFraction)

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
