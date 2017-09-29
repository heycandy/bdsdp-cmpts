package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import com.chinasofti.ark.bdadp.component.api.source.SourceComponent
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.slf4j.Logger
import org.jblas.DoubleMatrix

/**
  * Created by Hu on 2017/7/17.
  */
class ALSProductsRecommend (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData,SparkData](id, name, log) with Configureable with Serializable {

  var path: String = null
  var productsArr: Array[String] = null
  var topK: Int = 0

  override def apply(inputT: SparkData): SparkData = {
    val sc = inputT.getRawData.sqlContext.sparkContext

    val df = inputT.getRawData

    val sameModel = MatrixFactorizationModel.load(sc, path)
 //   var itemFactor :Array[Double] = null
//    var itemVector :DoubleMatrix = null
//    var sims :RDD[(Int,Double)] = null
   /* for(i <- Range(0, productsArr.length)){
      itemFactor ++= sameModel.productFeatures.lookup(productsArr(i).toInt).head
    }

    val factorRDD = sc.parallelize(itemFactor)*/
    for(i <- 0 until productsArr.length){
      println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" )
      val itemFactor = sameModel.productFeatures.lookup(productsArr(i).toInt).head
      val itemVector = new DoubleMatrix(itemFactor)
      val itemVectorBc = sc.broadcast(itemVector)
      val sims = sameModel.productFeatures.map{ case (id, factor) =>
         val factorVector = new DoubleMatrix(factor)
        // val sim = cosineSimilarity(factorVector, itemVectorBc.value)
        val sim =factorVector.dot(itemVectorBc.value) /(factorVector.norm2() * itemVectorBc.value.norm2())
          (id, sim)
        }

      sims.collect() foreach println
      println("<<<<<<<<: sims.count  " +  sims.count())
      val sortedSims :Array[(Int,Double)]= sims.top(topK)(Ordering.by[(Int, Double), Double]{
        case (id, similarity) => similarity
      })
      info("<<<<<similarity products of the: " + productsArr(i))
      info("-----------(productID, similarity)")
      sortedSims.foreach(row => info(row.toString()))
    }

    Builder.build(df)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    productsArr = componentProps.getString("productsArr").split(",")
    topK = componentProps.getString("topK").toInt
  }

  //judge the similarity between the products
  @transient
  def cosineSimilarity(vec1:DoubleMatrix,vec2:DoubleMatrix):Double = {
    vec1.dot(vec2) /(vec1.norm2() * vec2.norm2())
  }

}
