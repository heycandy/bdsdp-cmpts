package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger

/**
 * Created by Administrator on 2017.1.18.
 */
class Derive (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var newColName : String = null
  var conditionExprs: String = null
  var values : String = null
  var defaultValue : String = null
  var delimiter: String = null

  override def apply(inputT: SparkData): SparkData = {
    val df = inputT.getRawData
    val exprsArr = conditionExprs.split(delimiter)
    val valArr = values.split(delimiter)

    var expression  = "( CASE"
    for(i <- 0 to exprsArr.length -1){
      expression += " WHEN " + exprsArr(i) + "  THEN " +  valArr(i)
    }

    expression += " ELSE " + defaultValue + " END ) " + newColName
    log.debug("expression ====== " + expression)
    Builder.build(df.selectExpr("*",expression))

  }

  override def configure(componentProps: ComponentProps): Unit = {
    newColName = componentProps.getString("newColName")
    conditionExprs = componentProps.getString("conditionExprs")
    values = componentProps.getString("values")
    defaultValue = componentProps.getString("defaultValue")
    delimiter = componentProps.getString("delimiter",",")
    StringUtils.assertIsBlank(newColName,conditionExprs,values,defaultValue);
  }

}