package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger


class Drop(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var colsName: String = null
  val regex: String = ","

  override def apply(inputT: SparkData): SparkData = {
    var df = inputT.getRawData
    val cols = colsName.split(regex)
    for (col <- cols) {
      df = df.drop(col)
    }
    Builder.build(df)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    colsName = componentProps.getString("colsName")
    StringUtils.assertIsBlank(colsName)
  }
}