package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger

/**
  * Created by Administrator on 2017/1/16.
  */
class GroupBy (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {


  var colNames: String = null
  var aggExpr: String = null
  var key:String = null
  var value:String = null
  var strs = Array[String]()
  var treasureMap = Map[String, String]()

  override def apply(inputT: SparkData): SparkData = {
    strs = aggExpr.split(",")
    for(a <- strs){
      key = a.substring(a.indexOf("(")+1,a.indexOf(")"))
      value = a.substring(0,a.indexOf("("))
      treasureMap += (key -> value)
    }

    val df: DataFrame = inputT.getRawData.groupBy(colNames.split(",")(0),(colNames.split(",").tail): _*)
      .agg(treasureMap.head,(treasureMap.toSeq.tail): _*)
    Builder.build(df)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    colNames = componentProps.getString("colNames");
    aggExpr = componentProps.getString("aggExpr");

    StringUtils.assertIsBlank(colNames,aggExpr)
  }
}
