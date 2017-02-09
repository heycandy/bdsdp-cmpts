package com.chinasofti.ark.bdadp.component

import java.util.Random

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.slf4j.Logger

/**
 * Created by Administrator on 2017/1/12.
 */
class Sample(id: String, name: String, log: Logger)
    extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var withReplacement: Boolean = true
  var fraction: Double = 0.00
  var seed: Long = 0

  override def apply(inputT: SparkData): SparkData = {
    Builder.build(inputT.getRawData.sample(withReplacement, fraction, seed))
  }

  override def configure(componentProps: ComponentProps): Unit = {
    withReplacement = componentProps.getString("withReplacement").toBoolean;
    fraction = componentProps.getString("fraction").toDouble;
    seed = componentProps.getString("seed").toLong;

    if (null != seed && seed != 0) {
      seed = componentProps.getString("seed").toLong
    };
    else {
      seed = new Random().nextLong()
    };
  }
}
