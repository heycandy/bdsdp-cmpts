package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.slf4j.Logger

/**
  * Created by Hu on 2017/6/30.
  */
class CollaborativeFilteringALS (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
    SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null
 /* var trainDataSplit:Double = 0.0
  var testDataSplit:Double = 0.0*/
  var numClusters:Int = 0
  var numIterations:Int = 0

  override def configure(componentProps: ComponentProps):Unit = {
    path = componentProps.getString("path")
  /*  trainDataSplit = componentProps.getString("trainDataSplit", "1").toDouble
    testDataSplit = componentProps.getString("testDataSplit", "0").toDouble*/
    numClusters = componentProps.getString("numClusters","3").toInt
    numIterations = componentProps.getString("numIterations", "20").toInt
  }

  override def apply(inputT: StringData): Unit ={

  }

  //  val parseData = data.map(row => Vectors.dense(row.split(' ').map(_.toDouble))).cache()
  override def apply(inputT: SparkData): Unit = {
    val ratings = inputT.getRawData.mapPartitions(iterator => iterator.map(row => {

      row.toSeq.toVector.map({
        case (user,item,rate) => (user.toString,item.toString,rate.toString)
      }).map(f => f match {
        case (user,item,rate) =>
          Rating(user.toInt,item.toInt,rate.toDouble)
      })
    }))
/*
    println("<<<<<<<<<<<<out<<<<<<<<<<<<<<<<<<<,")
    ratings.collect foreach(println(_))
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    ALS.train(ratings,1,1,0.01)*/
    // val sameModel = KMeansModel.load(sc, "myModelPath")
  }
}
