package com.chinasofti.ark.bdadp.component

import java.util

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.slf4j.Logger

/**
 * Created by White on 2017/3/16.
 */
class PieChartSink(id: String, name: String, log: Logger)
    extends SinkComponent[StringData](id, name, log) with Configureable with
            SparkSinkAdapter[SparkData] with Serializable {

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
    val chartData = inputT.getRawData.select(nameColumn, valueColumn)
    val legendData = chartData.map(_ (0).toString).collect()
    val seriesData = chartData
//        .map(f => ImmutableMap.of("name", f(0).toString, "value", f(1).toString.toDouble))
        .map(f => {
          val map = new util.HashMap[String, Object]()

          map.put("name", f(0).toString)
          map.put("value", java.lang.Double.valueOf(f(1).toString))

          map
        }).collect()

    //    val map = ImmutableMap.builder[String, Object]()
    //        .put("title", title)
    //        .put("subtitle", subtitle)
    //        .put("legendData", legendData)
    //        .put("seriesData", seriesData)
    //        .build()

    val map = new util.HashMap[String, Object]()

    map.put("title", title)
    map.put("subtitle", subtitle)
    map.put("legendData", legendData)
    map.put("seriesData", seriesData)

    chart("pie.vm", map)

  }
}
