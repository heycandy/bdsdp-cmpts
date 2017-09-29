package com.chinasofti.ark.bdadp.component

import java.util
import java.util.zip.CRC32

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.transforms.MultiTransComponent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.slf4j.Logger

/**
  * Created by White on 2017/3/16.
  */
class CycleChartSink(id: String, name: String, log: Logger)
  extends MultiTransComponent[util.Collection[SparkData], StringData](id, name, log) with Configureable with Serializable {

  var sourceCol: String = null
  var destinationCol: String = null
  var attrCol:String = null
  var title: String = null
  var subtitle: String = null


  override def configure(componentProps: ComponentProps): Unit = {
    title = componentProps.getString("title", "")
    subtitle = componentProps.getString("subtitle", "")
    sourceCol = componentProps.getString("sourceCol","C1")
    destinationCol = componentProps.getString("destinationCol","C2")
    attrCol = componentProps.getString("attrCol","")
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

    val rawDF = iteratorDF.next().getRawData
    val edgesDF = iteratorDF.next().getRawData
    val sqlContext = rawDF.sqlContext
    val sc = sqlContext.sparkContext
    val expr = sourceCol ::destinationCol :: attrCol ::  Nil
    val sourceColBc = sc.broadcast(sourceCol)
    val destinationBc = sc.broadcast(destinationCol)
    val attrColBc = sc.broadcast(attrCol)
    val mapRDD:RDD[Row] = rawDF.selectExpr(expr:_*).mapPartitions(iterator => iterator.map(row =>{
      val Array(c0, c1) = Array(row.getAs(sourceColBc.value).toString,row.getAs(destinationBc.value).toString)
      val Crc322 = new CRC32
      Crc322.update(c0.getBytes)
      val c2 = Crc322.getValue

      val Crc323 = new CRC32
      Crc323.update(c1.getBytes)
      val c3 = Crc323.getValue
      Row(c0,c1,c2,c3,row.getAs(attrColBc.value).toString.toDouble)
    })).cache()
    val structType = StructType(Array(
      StructField(sourceCol, StringType, true),
      StructField(destinationCol,StringType,true),
      StructField("srcId#",LongType,true),
      StructField("dstId#",LongType,true),
      StructField(attrCol,DoubleType,true)
    ))
   val mapDF =  sqlContext.createDataFrame(mapRDD,structType).toDF()
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<mapDF: ")
    mapDF.show(10)
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<edgesDF show")
    edgesDF.show(10)

   val resultDF = edgesDF.join(mapDF,edgesDF("srcId") === mapDF("srcId#") &&
      edgesDF("dstId") === mapDF("dstId#")).select("srcId",sourceCol,"dstId",destinationCol,attrCol)

  resultDF.show()

    var verticesDF:DataFrame = null
    if(iteratorDF.hasNext){
      verticesDF = iteratorDF.next().getRawData
      println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<verticesDF: ")
      verticesDF.show()
    val vertices = verticesDF.map(row => (row(0).toString.toLong, row(1).toString)).collect().toMap
    val verticesBc = sc.broadcast(vertices)
      val seriesData = resultDF.mapPartitions(iterator =>iterator.map({
        f => (f.getAs(sourceColBc.value).toString,f.getAs(destinationBc.value).toString)
      })).flatMap(f => Array(f._1, f._2)).distinct().map(
        f => {
          val map = new util.HashMap[String, Object]()
           val name = verticesBc.value.getOrElse(f.toLong, "Unknown")

          map.put("name", name)

          map
        }
      ).collect()


      val seriesLinks = resultDF.map(row => {
        val map = new util.HashMap[String, Object]()

        val srcId = row.getAs(sourceColBc.value).toString
        val dstId = row.getAs(destinationBc.value).toString
        val value = row.getAs(attrColBc.value).toString
        val source = verticesBc.value.getOrElse(srcId.toLong, "Unknown")
        val target = verticesBc.value.getOrElse(dstId.toLong, "Unknown")
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
    }else{
      val seriesData = resultDF.mapPartitions(iterator =>iterator.map({
        f => (f.getAs(sourceColBc.value).toString,f.getAs(destinationBc.value).toString)
      })).flatMap(f => Array(f._1, f._2)).distinct().map(
        f => {
          val map = new util.HashMap[String, Object]()
          // val name = verticesBc.value.getOrElse(f, "Unknown")

          map.put("name", f.toString)

          map
        }
      ).collect()


      val seriesLinks = resultDF.map(row => {
        val map = new util.HashMap[String, Object]()

        val srcId = row.getAs(sourceColBc.value).toString
        val dstId = row.getAs(destinationBc.value).toString
        val value = row.getAs(attrColBc.value).toString

        map.put("source", srcId)
        map.put("target", dstId)
        map.put("value", value)

        map
      }).collect()
      val map = new util.HashMap[String, Object]()

      map.put("title", title)
      map.put("subtitle", subtitle)
      map.put("seriesData", seriesData)
      map.put("seriesLinks", seriesLinks)

      chart("cycle.vm", map)
    }

    Builder.build("Done")

  }

}


/*class CycleChartSink(id: String, name: String, log: Logger)
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

}*/
