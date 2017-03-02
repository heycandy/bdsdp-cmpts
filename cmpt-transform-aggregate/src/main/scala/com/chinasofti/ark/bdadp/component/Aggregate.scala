package com.chinasofti.ark.bdadp.component


import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.apache.spark.sql.{Column, DataFrame, GroupedData}
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/1/16.
  */
class Aggregate (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {


  var aggExpr: String = null
  var newName: String = null
  var delimiter: String = null
  var strsAgg = Array[String]()
  var strsNew = Array[String]()
  var tmpStr: String = null
  var resultAgg: String = null
  var i:Int = 0

  override def apply(inputT: SparkData): SparkData = {
    strsAgg = aggExpr.split(delimiter)
    strsNew = newName.split(delimiter)

    for(a <- strsAgg){
      tmpStr = tmpStr + a + " " + "as" + " " + strsNew(i) + ","
      i+=1
    }
    resultAgg = tmpStr.substring(4,tmpStr.length-1)
    info("resultAgg is: "+resultAgg)
    val df: DataFrame = inputT.getRawData.selectExpr(resultAgg.split(","): _*)
    Builder.build(df)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    aggExpr = componentProps.getString("aggExpr")
    newName = componentProps.getString("newName")
    delimiter = componentProps.getString("delimiter",",")
  }
}
