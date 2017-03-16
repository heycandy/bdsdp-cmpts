package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import com.google.common.collect.ImmutableMap
import org.slf4j.Logger

/**
 * Created by White on 2017/3/16.
 */
class PieSink(id: String, name: String, log: Logger)
    extends SinkComponent[StringData](id, name, log) with Configureable with
            SparkSinkAdapter[SparkData] {

  var title: String = null
  var subtitle: String = null
  var nameColumn: String = null
  var valueColumn: String = null

  override def apply(inputT: StringData): Unit = {

  }

  override def configure(componentProps: ComponentProps): Unit = {
    title = componentProps.getString("title", "")
    subtitle = componentProps.getString("subtitle", "")
    nameColumn = componentProps.getString("nameColumn", "name")
    valueColumn = componentProps.getString("valueColumn", "value")
  }

  /**
   *
   * @param inputT
   *
   * | name | value |
   * ----------------
   * |  n1  |   1  |
   * |  n2  |   2  |
   * |  n3  |   3  |
   */
  override def apply(inputT: SparkData): Unit = {
    val legendData = inputT.getRawData.select(nameColumn)
        .map(_ (0).toString).collect()
    val seriesData = inputT.getRawData.select(nameColumn, valueColumn)
        .map(f => ImmutableMap.of("name", f(0).toString, "value", f(1).toString.toDouble))
        .collect()

    val entrySet = ImmutableMap.builder[String, Object]()
        .put("title", title)
        .put("subtitle", subtitle)
        .put("legendData", legendData)
        .put("seriesData", seriesData)
        .build()

    chart("pie.vm", entrySet)

  }
}
