package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

class Drop(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var colsName: String = null

  var dropCols = new ArrayBuffer[String]()
  var i: Int = 0

  override def apply(inputT: SparkData): SparkData = {
    var df = inputT.getRawData
    for (m <- 0 until colsName.split(",").length - 1) {
      dropCols += colsName.split(",")(m)
    }

    for (a <- dropCols) {
      df = df.drop(dropCols(i))
      i += 1
    }
    Builder.build(df)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    colsName = componentProps.getString("colsName")
    StringUtils.assertIsBlank(colsName)
  }
}