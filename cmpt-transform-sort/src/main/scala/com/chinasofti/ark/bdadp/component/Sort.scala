package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger;

/**
 * Created by Administrator on 2017.1.11.
 */
class Sort(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var colName: String = null;
  var sortDirection: String = null;

  override def apply(inputT: SparkData): SparkData = {
    val df = inputT.getRawData
    var buildDataFrame: DataFrame = null
    if ("desc".equalsIgnoreCase(sortDirection)) {
      buildDataFrame = df.sort(df.col(colName).desc)
    }
    else {
      buildDataFrame = df.sort(df.col(colName).asc)
    }
    Builder.build(buildDataFrame)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    colName = componentProps.getString("colName");
    sortDirection = componentProps.getString("sortDirection");

    StringUtils.assertIsBlank(colName);
  }

  def call(inputT: SparkData, cmptProps: ComponentProps): SparkData = {
    configure(cmptProps)
    apply(inputT)
  }

  def call(inputT: SparkData, colName: String, sortDirection: String): SparkData = {
    val cmptProps = new ComponentProps()
    cmptProps.setProperty("colName", colName)
    cmptProps.setProperty("sortDirection", sortDirection)
    configure(cmptProps)
    apply(inputT)
  }
}
