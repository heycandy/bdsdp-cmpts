package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.slf4j.Logger

/**
 * Created by Administrator on 2017.1.18.
 */
class Derive (id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var conditionExpr: String = null
  var newColName : String = null
  var positiveResult : String = null
  var negativeResult : String = null

  override def apply(inputT: SparkData): SparkData = {
    val df = inputT.getRawData
    val expression : String = "( CASE WHEN " + conditionExpr + "  THEN " + positiveResult + " ELSE "+ "'" + negativeResult + "'"+ " END ) " + newColName
    Builder.build(df.selectExpr("*",expression))

  }

  override def configure(componentProps: ComponentProps): Unit = {
    conditionExpr = componentProps.getString("conditionExpr")
    newColName = componentProps.getString("newColName")
    positiveResult = componentProps.getString("positiveResult")
    negativeResult = componentProps.getString("negativeResult")
  }

}