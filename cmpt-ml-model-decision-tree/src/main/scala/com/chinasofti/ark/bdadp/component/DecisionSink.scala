package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.slf4j.Logger


/**
 * Created by Administrator on 2017.2.16.
 */
class DecisionSink(id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] with Serializable {

  var path: String = null
  var numClasses: Int = 0
  var impurity: String = null
  var maxDepth: Int = 0
  var maxBins: Int = 0

  override def apply(inputT: StringData): Unit = {
    //    info(inputT.getRawData)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    path = componentProps.getString("path")
    numClasses = componentProps.getString("numClasses", "2").toInt
    impurity = componentProps.getString("impurity", "gini")
    maxDepth = componentProps.getString("maxDepth", "5").toInt
    maxBins = componentProps.getString("maxBins", "32").toInt
  }

  override def apply(inputT: SparkData): Unit = {
    // {label, 0, 1, 2, 3, ...}
    val df = inputT.getRawData
    val data = df.mapPartitions(iterator => iterator.map(row => {
      val label = row.toSeq.head.toString.toDouble
      val values = row.toSeq.tail.map(_.toString).map(_.toDouble).toArray

      val features = Vectors.dense(values)
      new LabeledPoint(label, features)
    }))

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testAccuracy = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testData.count()
    info("Test Accuracy = " + testAccuracy)
    //    info("Learned classification tree model:\n" + model.toDebugString)

    // Save and load model
    val sc = df.sqlContext.sparkContext
    model.save(sc, path)
  }
}
