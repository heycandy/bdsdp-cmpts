package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.slf4j.Logger

/**
 * Created by White on 2017/1/4.
 */
class Filter(id: String, name: String, log: Logger)
    extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var conditionExpr: String = null

  override def apply(inputT: SparkData): SparkData = {
    Builder.build(inputT.getRawData.filter(conditionExpr))
  }

  override def configure(componentProps: ComponentProps): Unit = {
    conditionExpr = componentProps.getString("conditionExpr")
  }
}
