package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2017.1.12.
 */
class Alias(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var existingName: String = null
  var newName: String = null

  var strsExist = new ArrayBuffer[String]()
  var strsNew = ArrayBuffer[String]()
  var i:Int = 0

  override def apply(inputT: SparkData): SparkData = {
    var df = inputT.getRawData
    for(m <- 0 until existingName.split(",").length-1){
      strsNew += existingName.split(",")(m)
    }

    for(a <- strsExist){
      df = df.withColumnRenamed(a, strsNew(i))
      i+=1
    }
    Builder.build(df)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    existingName = componentProps.getString("existingName")
    newName = componentProps.getString("newName")
    StringUtils.assertIsBlank(existingName, newName);
  }

  def call(inputT: SparkData, cmptProps: ComponentProps): SparkData = {
    configure(cmptProps)
    apply(inputT)
  }

  def call(inputT: SparkData, existingName: String, newName: String): SparkData = {

    val cmptProps = new ComponentProps()
    cmptProps.setProperty("existingName", existingName)
    cmptProps.setProperty("newName", newName)

    configure(cmptProps)
    apply(inputT)
  }
}
