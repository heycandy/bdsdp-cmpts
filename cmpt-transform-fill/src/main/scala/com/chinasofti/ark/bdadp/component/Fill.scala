package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger

/**
  * Created by Administrator on 2017.2.9.
  */
class Fill(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var value: String = null
  var cols: Array[String] = null
  var valueType: String = null
  var delimiter: String = null

  override def apply(inputT: SparkData): SparkData = {
    val df = inputT.getRawData
    var fillDF: DataFrame = null

    if ("string".equalsIgnoreCase(valueType))
      fillDF = df.na.fill(value, cols)
    else
      fillDF = df.na.fill(value.toDouble, cols)

    Builder.build(fillDF)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    val colsStr = componentProps.getString("cols")
    delimiter = componentProps.getString("delimiter")
    valueType = componentProps.getString("valueType")
    value = componentProps.getString("value")

    StringUtils.assertIsBlank(colsStr, delimiter, valueType, value)
    cols = colsStr.split(delimiter)

  }
}
