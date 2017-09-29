package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
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
  var trainDataSplit:Double = 0.0
  var rank:Int = 0
  var numIterations:Int = 0

  override def configure(componentProps: ComponentProps):Unit = {
    path = componentProps.getString("path")
    trainDataSplit = componentProps.getString("trainDataSplit", "1").toDouble
    rank = componentProps.getString("numClusters","10").toInt
    numIterations = componentProps.getString("numIterations", "10").toInt
  }

  override def apply(inputT: StringData): Unit ={

  }

  override def apply(inputT: SparkData): Unit = {
    val df = inputT.getRawData
    df.show()
    val raw_ratings: RDD[Rating] = df.mapPartitions(itertor => itertor.map(row =>row match{
      case Row(user,item,rate) => Rating(user.toString.toInt,item.toString.toInt,rate.toString.toDouble)
    }))
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<ratings<<<<<<<<<<<<<")
    raw_ratings.collect() foreach println
    val splits = raw_ratings.randomSplit(Array(trainDataSplit,1-trainDataSplit))
    val (ratings, testData) = (splits(0), splits(1))
    // Build the recommendation model using ALS
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }


    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    info("Mean Squared Error = " + MSE)
    val sc = inputT.getRawData.sqlContext.sparkContext
    model.save(sc,path)

    info("userFeatures num: " + model.userFeatures.count())
    info("productFeatures num: " + model.productFeatures.count)

    info("predictions: ")
    predictions.collect().foreach(row => info(row.toString()))

  //  val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
  }
}
