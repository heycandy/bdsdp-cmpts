
package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.slf4j.Logger

class Distinct(id: String, name: String, log: Logger)
    extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  override def apply(inputT: SparkData): SparkData = {
    Builder.build(inputT.getRawData.distinct)
  }

  override def configure(componentProps: ComponentProps): Unit = {
  }


}
