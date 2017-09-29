package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger;

/**
 * Created by Administrator on 2017.1.11.
 */
class Sort(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var colName: String = null;
  var sortDirection: String = null;

  override def apply(inputT: SparkData): SparkData = {
    var df = inputT.getRawData
    val col = df.col(colName)
    if ("desc".equalsIgnoreCase(sortDirection)) {
      df = df.sort(col.desc)
    }
    else {
      df = df.sort(col.asc)
    }
    Builder.build(df)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    colName = componentProps.getString("colName");
    sortDirection = componentProps.getString("sortDirection");

    StringUtils.assertIsBlank(colName);
  }
}
