package com.chinasofti.ark.bdadp.component

import java.util

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.MultiTransComponent
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger

import scala.collection.JavaConversions._

/**
  * Created by Hu on 2017/1/12.
  */
class Join(id: String, name: String, log: Logger)
    extends MultiTransComponent[util.Collection[SparkData], SparkData](id, name, log) with
            Configureable {


  var joinExprs: String = _

  /**
    * `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
    */
  var joinType: String = _

  override def apply(inputT: util.Collection[SparkData]): SparkData = {

    /**
      * @since 1.3.0
      */
    val join = (left: DataFrame, right: DataFrame) => left.join(right)

    /**
      * @since 1.4.0
      */
    val joinUsingColumns = (left: DataFrame, right: DataFrame) => {
      val usingColumns = joinExprs.replace(" ", "").split(",")

      left.join(right, usingColumns)
    }

    /**
      * @since 1.6.0
      */
    val joinUsingColumnsWithType = (left: DataFrame, right: DataFrame) => {
      val usingColumns = joinExprs.replace(" ", "").split(",")

      left.join(right, usingColumns, joinType)
    }

    var rawData: DataFrame = null

    if (joinExprs.isEmpty) {
      rawData = inputT.map(_.getRawData).reduce(join)

    } else if (joinExprs.nonEmpty && joinType.isEmpty) {
      rawData = inputT.map(_.getRawData).reduce(joinUsingColumns)

    } else if (joinExprs.nonEmpty && joinType.nonEmpty) {
      rawData = inputT.map(_.getRawData).reduce(joinUsingColumnsWithType)

    }

    Builder.build(rawData)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    joinExprs = componentProps.getString("joinExprs", "")
    joinType = componentProps.getString("joinType", "")

  }

}
