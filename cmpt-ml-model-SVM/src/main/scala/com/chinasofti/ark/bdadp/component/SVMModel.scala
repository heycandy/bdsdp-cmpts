package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SparkSinkAdapter, SinkComponent}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger

/**
 * Created by water on 2017.6.28.
 */
class SVMModel(id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] with Serializable {

  var filePath: String = null
  var path: String = null
  var iters: Int = 0
  var trainDataPer: Double = 0.0
  var regParam: Double = 0
  var seed: Long = 0
  var step: Double = 0.0
  var fraction: Double = 0.0

  override def apply(inputT: StringData): Unit = {
  }

  override def configure(componentProps: ComponentProps): Unit = {
    filePath = componentProps.getString("filePath")
    path = componentProps.getString("path")
    iters = componentProps.getString("iters", "100").toInt
    trainDataPer = componentProps.getString("trainDataPer", "0.7").toDouble
    regParam = componentProps.getString("regParam", "0.0").toDouble
    seed = componentProps.getString("seed",(new util.Random).nextLong().toString).toLong
    step = componentProps.getString("step", "1.0").toDouble
    fraction = componentProps.getString("fraction", "1.0").toDouble

  }

  override def apply(inputT: SparkData): Unit = {
    val df = inputT.getRawData

    printInput(df)
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

    /*
     * stepSize: 迭代步长，默认为1.0
     * numIterations: 迭代次数，默认为100
     * regParam: 正则化参数，默认值为0.0
     * miniBatchFraction: 每次迭代参与计算的样本比例，默认为1.0
     * gradient：HingeGradient ()，梯度下降；
     * updater：SquaredL2Updater ()，正则化，L2范数；
     * optimizer：GradientDescent (gradient, updater)，梯度下降最优化计算。
     */
    val model = SVMWithSGD.train(trainingData, iters, step, fraction)
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testAccuracy = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testData.count
    info("Test Accuracy = " + testAccuracy)


    val sc = df.sqlContext.sparkContext
    model.save(sc, path)

  }

  def printInput(df: DataFrame): Unit = {
    ("" :: df.toString() ::
      Nil ++ df.repartition(8).take(10)).foreach(row => info(row.toString()))
  }
}
