package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.slf4j.Logger

/**
  * Created by Hu based on spark-ml.
  */
class K_means (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
    SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null
  var trainDataSplit:Double = 0.0
  var testDataSplit:Double = 0.0
  var numClusters:Int = 0
  var numIterations:Int = 0
  var features:Array[String] =null
  var predictionColName:String = null

  override def configure(componentProps: ComponentProps):Unit = {
    path = componentProps.getString("path")
    trainDataSplit = componentProps.getString("trainDataSplit", "1").toDouble
    testDataSplit = componentProps.getString("testDataSplit", "0").toDouble
    numClusters = componentProps.getString("numClusters","3").toInt
    numIterations = componentProps.getString("numIterations", "33").toInt
    features = componentProps.getString("features","C0").split(",")
    predictionColName = componentProps.getString("predictionColName","kmeans_prediction")
  }

  override def apply(inputT: StringData): Unit ={

  }

//  val parseData = data.map(row => Vectors.dense(row.split(' ').map(_.toDouble))).cache()
  override def apply(inputT: SparkData): Unit = {

  val df = inputT.getRawData
  info("------------data input:")
  df.repartition(8).take(10).foreach(row => info(row.toString()))
  val assembler = new VectorAssembler()
    .setInputCols(features)
    .setOutputCol("features")

  val splits = df.randomSplit(Array(trainDataSplit,testDataSplit))
  val (trainingData, testData) = (splits(0), splits(1))
  val output = assembler.transform(trainingData)

  // Trains a k-means model
  val kmeans = new KMeans()
    .setK(numClusters)
    .setFeaturesCol("features")
    .setPredictionCol(predictionColName)
    .setMaxIter(numIterations)

  val model = kmeans.fit(output)

  // Shows the result
  info("Final Centers: ")
  model.clusterCenters.foreach(row => info(row.toString))

  // Save and load model
  info("----taking 10 item of samples of K-means Cluster result: ")
  val transformed = model.transform(output)

  transformed.select("features",predictionColName).repartition(8).take(10).foreach(row => info(row.toString()))
  val clusters = transformed.select(predictionColName).map(_.getInt(0)).distinct().collect().toSet

  info("-----predict clusters value:")
  clusters.foreach(row => info(row.toString))
  /**
    * Return the K-means cost (sum of squared distances of points to their nearest center) for this
    * model on the given data.
    */
  info("the K-means cost(sum of squared distances of points to their nearest center): ")
  info(model.computeCost(output).toString)

  model.save(path)
  }
}


/**
  * based on the spark mllib realization 7/7
  */
/*class K_means (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
    SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null
  var trainDataSplit:Double = 0.0
  var testDataSplit:Double = 0.0
  var numClusters:Int = 0
  var numIterations:Int = 0

  override def configure(componentProps: ComponentProps):Unit = {
    path = componentProps.getString("path")
    trainDataSplit = componentProps.getString("trainDataSplit", "1").toDouble
    testDataSplit = componentProps.getString("testDataSplit", "0").toDouble
    numClusters = componentProps.getString("numClusters","3").toInt
    numIterations = componentProps.getString("numIterations", "20").toInt
  }

  override def apply(inputT: StringData): Unit ={

  }

  //  val parseData = data.map(row => Vectors.dense(row.split(' ').map(_.toDouble))).cache()
  override def apply(inputT: SparkData): Unit = {
    val data = inputT.getRawData.mapPartitions(iterator => iterator.map(row => {
      val parts = row.toSeq
      Vectors.dense(parts.toArray.map(_.toString).map(_.toDouble))
    }))

    val splits = data.randomSplit(Array(trainDataSplit,testDataSplit))
    val (trainingData, testData) = (splits(0), splits(1))

    //Cluster the data into two classes using KMeans
    val clustersModel = KMeans.train(trainingData,numClusters,numIterations)
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clustersModel.computeCost(data)
    info("Within Set Sum of Squared Errors = " + WSSSE)
    info("--clusterCenters:")
    clustersModel.clusterCenters.foreach(cc => info(cc.toString))


    // Save and load model
    /*info("<<<<<<<<<<<<<<<<<<<<testData predict:  ")
    trainingData.foreach({
      vec =>clustersModel.predict(vec) + ": " + vec
    })*/
    info("----taking 10 items of sample K-means Cluster result: ")
    val predictData = trainingData.map({
      vec => clustersModel.predict(vec) + ": " + vec
    })

    predictData.take(10).foreach(row => info(row))
    val sc = inputT.getRawData.sqlContext.sparkContext
    clustersModel.save(sc, path)
    // val sameModel = KMeansModel.load(sc, "myModelPath")
  }
}*/
