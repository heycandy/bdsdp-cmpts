package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

/**
 * Created by water on 2017.2.9.
 */
class Fill(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var origins: Array[String] = _
  var fills: Array[String] = _
  var cols: Array[String] = _
  var delimiter: String = _

  val REGEX = ","
  val NULL = "null"

  override def apply(inputT: SparkData): SparkData = {

    var df = inputT.getRawData
    for (i <- 0 until cols.length) {
      if (StringUtils.equals(NULL, origins(i))) {
        df = df.na.fill(Map(cols(i) -> fills(i)))
      }
      else {
        df = df.na.replace(cols(i), Map(origins(i) -> fills(i)))
      }
    }
    Builder.build(df)
  }

  override def configure(cmptPs: ComponentProps): Unit = {
    val originsStr = cmptPs.getString("origins")
    val fillsStr = cmptPs.getString("fills")
    val colsStr = cmptPs.getString("cols")
    delimiter = cmptPs.getString("delimiter", REGEX)

    origins = StringUtils.trimSplit(originsStr)
    fills = StringUtils.trimSplit(fillsStr)
    cols = StringUtils.trimSplit(colsStr)
  }
}
