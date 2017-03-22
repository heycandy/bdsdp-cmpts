package com.chinasofti.ark.bdadp.component

import java.util.Random

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

/**
  * Created by Administrator on 2017/1/12.
  */
class Sample(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var samplingType: String = null
  var withReplacement: Boolean = true
  var fractionOrNum: String = null
  var seed: Long = 0

  override def apply(inputT: SparkData): SparkData = {
    val sc = inputT.getRawData.sqlContext.sparkContext
    val df = inputT.getRawData
    //compute with the fraction or line num.
    if("fraction".equalsIgnoreCase(samplingType)){
      Builder.build(df.sample(withReplacement, fractionOrNum.toDouble,seed))
    }else
    {
      val schema = df.schema
      val sampleArray = df.rdd.takeSample(withReplacement,fractionOrNum.toInt,seed)
      //Parallel array and build rdd
      val sampleRDD = sc.parallelize(sampleArray)
      val dfResult = inputT.getRawData.toDF().sqlContext.createDataFrame(sampleRDD, schema)
      Builder.build(dfResult)
    }
  }

  override def configure(componentProps: ComponentProps): Unit = {
    samplingType = componentProps.getString("samplingType")
    fractionOrNum = componentProps.getString("fractionOrNum")
    val withReplacementStr = componentProps.getString("withReplacement")
    val seedStr = componentProps.getString("seed")

    StringUtils.assertIsBlank(withReplacementStr, fractionOrNum)
    withReplacement = withReplacementStr.toBoolean
    if (seedStr == null || seedStr.equals("")) {
      seed = new Random().nextLong()
    }
    else {
      seed = componentProps.getString("seed").toLong
    }
  }
}
