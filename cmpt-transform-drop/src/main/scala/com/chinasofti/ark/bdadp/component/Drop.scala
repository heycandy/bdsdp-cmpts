package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

class Drop(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var colName: String = null

  override def apply(inputT: SparkData): SparkData = {
    val df = inputT.getRawData
    Builder.build(df.drop(colName))
  }

  override def configure(componentProps: ComponentProps): Unit = {
    colName = componentProps.getString("colName")
    StringUtils.assertIsBlank(colName)
  }
}
