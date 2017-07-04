package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SparkSinkAdapter, SinkComponent}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame

import org.slf4j.Logger

/**
 * Created by water on 2017.6.26.
 */
class LogisticRegressionModel(id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] with Serializable {

  var filePath: String = null
  var path: String = null
  var numClasses: Int = 0
  var trainDataPer: Double = 0.0
  var seed: Long = 0

  override def apply(inputT: StringData): Unit = {
  }

  override def configure(componentProps: ComponentProps): Unit = {
    filePath = componentProps.getString("filePath")
    path = componentProps.getString("path")
    numClasses = componentProps.getString("numClasses", "2").toInt
    trainDataPer = componentProps.getString("trainDataPer", "0.7").toDouble
    seed = componentProps.getString("seed", (new util.Random).nextLong().toString).toLong

  }

  override def apply(inputT: SparkData): Unit = {
    //    http://blog.csdn.net/xubo245/article/details/51493958
    val df = inputT.getRawData

    ("" :: df.toString() ::
      Nil ++ df.repartition(8).take(10)).foreach(row => info(row.toString()))

    //    val parsedData = df.mapPartitions(iterator => iterator.map(row => {
    //      val label = row.toSeq.head.toString.toDouble
    //      val values = row.toSeq.tail.map(_.toString).map(_.toDouble).toArray
    //
    //      val features = Vectors.dense(values)
    //      new LabeledPoint(label, features)
    //    }))
    val parsedData = MLUtils.loadLibSVMFile(df.sqlContext.sparkContext, filePath).cache()

    val splits = parsedData.randomSplit(Array(trainDataPer, 1.0 - trainDataPer), seed)

    val (trainingData, testData) = (splits(0), splits(1))
    val model = new LogisticRegressionWithLBFGS().setNumClasses(numClasses).run(trainingData)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testAccuracy = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testData.count
    info("Test Accuracy = " + testAccuracy)

    val sc = df.sqlContext.sparkContext
    model.save(sc, path)

  }
}
