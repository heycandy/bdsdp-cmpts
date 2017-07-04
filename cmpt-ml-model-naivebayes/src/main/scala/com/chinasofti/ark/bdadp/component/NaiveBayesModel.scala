package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SparkSinkAdapter, SinkComponent}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger
import org.apache.commons.lang.StringUtils

/**
 * Created by water on 2017.6.28.
 */
class NaiveBayesModel(id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null
  var lambda: Double = 0
  var trainDataPer: Double = 0.0
  var modelType: String = null
  var seed: Long = 0

  override def apply(inputT: StringData): Unit = {
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    lambda = componentProps.getString("lambda", "0.0").toDouble
    trainDataPer = componentProps.getString("trainDataPer", "0.7").toDouble
    modelType = componentProps.getString("modelType")
    seed = componentProps.getString("seed", (new util.Random).nextLong().toString).toLong

  }

  override def apply(inputT: SparkData): Unit = {
//    http://www.cnblogs.com/jianjunyue/articles/5506139.html
    val df = inputT.getRawData

    printInput(df)

    val parsedData = df.mapPartitions(iterator => iterator.map(row => {
      val parts = StringUtils.strip(row.toString(),"[]").split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }))

    val splits = parsedData.randomSplit(Array(trainDataPer, 1.0 - trainDataPer), seed)

    val (trainingData, testData) = (splits(0), splits(1))

    val model = NaiveBayes.train(trainingData, lambda, modelType)

    // Compute raw scores on the test set.
    val predictionAndLabel= testData.map(p => (model.predict(p.features),p.label))
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(predictionAndLabel)
    val auROC = metrics.areaUnderROC()
    info("Area under ROC = " + auROC)

    val testAccuracy =1.0 *predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()
    info("Test Accuracy = " + testAccuracy)

    val sc = df.sqlContext.sparkContext
    model.save(sc, path)

  }

  def printInput(df: DataFrame): Unit = {
    ("" :: df.toString() ::
      Nil ++ df.repartition(8).take(10)).foreach(row => info(row.toString()))
  }
}

