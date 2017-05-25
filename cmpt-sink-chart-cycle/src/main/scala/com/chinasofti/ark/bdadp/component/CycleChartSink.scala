package com.chinasofti.ark.bdadp.component

import java.util

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.transforms.MultiTransComponent
import org.slf4j.Logger

/**
  * Created by White on 2017/3/16.
  */
class CycleChartSink(id: String, name: String, log: Logger)
  extends MultiTransComponent[util.Collection[SparkData], StringData](id, name, log) with Configureable with Serializable {

  var title: String = null
  var subtitle: String = null


  override def configure(componentProps: ComponentProps): Unit = {
    title = componentProps.getString("title", "")
    subtitle = componentProps.getString("subtitle", "")
  }

  /**
    * [1,2,200612.09]
    * [2,3,3205955.44]
    * [3,4,201008.30]
    * [4,1,201606.01]
    * *
    * [11,12,200612.09]
    * [11,13,3205955.44]
    * [13,14,201008.30]
    * [14,15,201606.01]
    * [15,16,3205955.48]
    * [16,11,201408.28]
    *
    * @param inputT
    * @return
    */
  override def apply(inputT: util.Collection[SparkData]): StringData = {
    val iteratorDF = inputT.iterator()
    val verticesDF = iteratorDF.next().getRawData
    val edgesDF = iteratorDF.next().getRawData

    val sqlContext = verticesDF.sqlContext
    val sc = sqlContext.sparkContext

    val vertices = verticesDF.map(row => (row(0).toString.toLong, row(1).toString)).collect().toMap
    val verticesBc = sc.broadcast(vertices)

    val seriesData = edgesDF.map(row => (row(0).toString.toLong, row(1).toString.toLong))
      .flatMap(f => Array(f._1, f._2)).distinct()
      .map(f => {
        val map = new util.HashMap[String, Object]()
        val name = verticesBc.value.getOrElse(f, "Unknown")

        map.put("name", name)

        map

      }).collect()

    val seriesLinks = edgesDF.map(row => {
      val map = new util.HashMap[String, Object]()

      val srcId = row(0).toString.toLong
      val dstId = row(1).toString.toLong
      val value = row(2).toString

      val source = verticesBc.value.getOrElse(srcId, "Unknown")
      val target = verticesBc.value.getOrElse(dstId, "Unknown")

      map.put("source", source)
      map.put("target", target)
      map.put("value", value)

      map
    }).collect()

    val map = new util.HashMap[String, Object]()

    map.put("title", title)
    map.put("subtitle", subtitle)
    map.put("seriesData", seriesData)
    map.put("seriesLinks", seriesLinks)

    chart("cycle.vm", map)

    Builder.build("Done")

  }

}
