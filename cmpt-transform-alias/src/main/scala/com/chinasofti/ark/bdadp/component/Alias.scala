package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

/**
 * Created by Administrator on 2017.1.12.
 */
class Alias(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var existingName: String = null
  var newName: String = null

  override def apply(inputT: SparkData): SparkData = {
    var df = inputT.getRawData
    val arrExist = existingName.split(",")
    val arrNew = newName.split(",")

    for (m <- 0 until arrExist.length) {
      df = df.withColumnRenamed(arrExist(m), arrNew(m))
    }
    Builder.build(df)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    existingName = componentProps.getString("existingName")
    newName = componentProps.getString("newName")
    StringUtils.assertIsBlank(existingName, newName);
  }
}
