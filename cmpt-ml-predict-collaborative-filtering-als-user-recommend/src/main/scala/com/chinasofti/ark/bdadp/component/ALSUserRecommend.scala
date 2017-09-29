package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.slf4j.Logger

/**
  * Created by Hu on 2017/7/16.
  */
  class ALSUserRecommend (id: String, name: String, log: Logger)
    extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

    var path: String = null
    var userArr: Array[String] = null
    var topKArr: Array[String] = null

    override def apply(inputT: SparkData): SparkData = {
      val sc = inputT.getRawData.sqlContext.sparkContext

      val df = inputT.getRawData
 /*     info("------------data input:")
      df.repartition(8).take(10).foreach(row => info(row.toString()))*/

      val sameModel = MatrixFactorizationModel.load(sc, path)
      var result:Array[Rating] = null
      if(userArr.length == topKArr.length){
        for(i <- 0 until userArr.length){
          result = sameModel.recommendProducts(userArr(i).toInt,topKArr(i).toInt)
        }
      }else if(userArr.length > topKArr.length){
        for(i <-0 until userArr.length){
          result = sameModel.recommendProducts(userArr(i).toInt,topKArr(0).toInt)
        }
      }
       info("user recommnend: ")
        result.foreach(row => row match {
        case Rating(user,item,rate) =>info((user,item,rate).toString)
      })


      Builder.build(df)
    }

    override def configure(componentProps: ComponentProps): Unit = {
      path = componentProps.getString("path")
      userArr = componentProps.getString("userArr").split(",")
      topKArr = componentProps.getString("topKArr").split(",")
    }
  }

