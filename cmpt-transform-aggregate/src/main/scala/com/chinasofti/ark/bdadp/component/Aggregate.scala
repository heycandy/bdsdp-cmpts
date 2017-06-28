package com.chinasofti.ark.bdadp.component


import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.apache.spark.sql.{Column, DataFrame, GroupedData}
import org.slf4j.Logger


/**
 * Created by Administrator on 2017/1/16.
 */
class Aggregate (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {


  var aggExpr: String = null
  var newName: String = null
  var delimiter: String = null

  override def apply(inputT: SparkData): SparkData = {
    val arrAgg = aggExpr.split(delimiter)
    val arrNew = newName.split(delimiter)
    var df = inputT.getRawData
    var tmpStr: String = null

    for(i <- 0 until arrAgg.length){
      tmpStr = tmpStr + arrAgg(i)  + " as " + arrNew(i) + ","
    }
    tmpStr = tmpStr.substring(4,tmpStr.length-1)
    info("resultAgg is: "+tmpStr)
    df = df.selectExpr(tmpStr.split(","): _*)

    for(m <- 0 until arrAgg.length){
      df = df.withColumnRenamed(arrAgg(m), arrNew(m))
    }

    Builder.build(df)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    aggExpr = componentProps.getString("aggExpr")
    newName = componentProps.getString("newName")
    delimiter = componentProps.getString("delimiter",",")

    StringUtils.assertIsBlank(aggExpr,newName);
  }
}
