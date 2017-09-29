package com.chinasofti.ark.bdadp.component

import java.util
import java.util.zip.CRC32

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.MultiTransComponent
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.slf4j.Logger
class CycleGraph(id: String, name: String, log: Logger)
    extends MultiTransComponent[util.Collection[SparkData], SparkData](id, name, log) with Configureable {

  var sourceCol: String = null
  var destinationCol: String = null
  var minDepth: Int = 3
  var maxDepth: Int = 0
 // var outPath: String = null

  override def configure(componentProps: ComponentProps): Unit = {
    sourceCol = componentProps.getString("sourceCol","C0")
    destinationCol = componentProps.getString("destinationCol","C1")
  //  minDepth = componentProps.getString("minDepth","3").toInt
    maxDepth = componentProps.getString("maxDepth","5").toInt
  //  outPath = componentProps.getString("outPath")
  }

  override def apply(inputT: util.Collection[SparkData]): SparkData = {

    val maxIterations = maxDepth + 1
    val rawData = inputT.iterator().next().getRawData

    val sqlContext = rawData.sqlContext
    import sqlContext.implicits._
     val sc = sqlContext.sparkContext
    val sourceColBc = sc.broadcast(sourceCol)
    val destinationBc = sc.broadcast(destinationCol)
    val mapRDD = rawData.map(f => {
        val Array(c0, c1) = Array(f.getAs(sourceColBc.value).toString, f.getAs(destinationBc.value).toString)
        val Crc322 = new CRC32
        Crc322.update(c0.getBytes)
        val c2 = Crc322.getValue

        val Crc323 = new CRC32
        Crc323.update(c1.getBytes)
        val c3 = Crc323.getValue

        (c0, c1, c2, c3)
      }).cache()

    val initialVertices = mapRDD.flatMap(f => Seq(f._3, f._4)).distinct().map(vertexId => (vertexId, false))
    val initialEdges= mapRDD.map(f => Edge(f._3, f._4, 1))

    mapRDD.unpersist(blocking = false)

    val graph = Graph(initialVertices, initialEdges, false).cache()

    // 过滤没有入度和出度的顶点
    var joinDegrees = graph.inDegrees.fullOuterJoin(graph.outDegrees).filter {
      case (vertexId, (inDegree, outDegree)) => inDegree.getOrElse(0) == 0 || outDegree.getOrElse(0) == 0
    }
    var joinCount = joinDegrees.count()
    var joinGraph = graph
    var subGraph = graph
    println("joinCount: " + joinCount)
    while (joinCount > 0) {
      val prevJoinGraph = joinGraph
      joinGraph = subGraph.outerJoinVertices(joinDegrees)((vertexId, attr, option) => option.isEmpty)

      val prevSubGraph = subGraph
      subGraph = joinGraph.subgraph(epred => true, (vertexId, attr) => attr).cache()

      val prevJoinDegrees = joinDegrees
      joinDegrees = subGraph.inDegrees.fullOuterJoin(subGraph.outDegrees).filter {
        case (vertexId, (inDegree, outDegree)) => inDegree.getOrElse(0) == 0 || outDegree.getOrElse(0) == 0
      }

      joinCount = joinDegrees.count()

      // 释放之前的资源
      prevJoinDegrees.unpersist(blocking = false)
      prevSubGraph.unpersist(blocking = false)
      prevJoinGraph.unpersist(blocking = false)
    }

    var mapGraph = subGraph.mapVertices((vertexId, attr) => Seq.empty[VertexId]).cache()

    // 释放之前的资源
    subGraph.unpersist(blocking = false)
    graph.unpersist(blocking = false)

    // 计算每个顶点可达的位置
    var messages = mapGraph.vertices
    var messagesCount = messages.count()
    var i = 0
    info("messagesCount: " + messagesCount)
    while (messagesCount > 0 && i < maxIterations) {

      val prevMessages = messages
      messages = mapGraph.aggregateMessages[Seq[VertexId]](
        ctx => if(ctx.dstAttr.isEmpty || !ctx.dstAttr.contains(ctx.dstId)) ctx.sendToDst(ctx.srcId +: ctx.srcAttr),
        (srcAttr, dstAttr) => (srcAttr ++ dstAttr).distinct
      ).cache()

      messagesCount = messages.count()

      val prevGraph = mapGraph
      mapGraph = mapGraph.outerJoinVertices(messages)((vertexId, left, right) => right.getOrElse(left)).cache()

      // 释放之前的资源
      prevMessages.unpersist(blocking = false)
      prevGraph.unpersist(blocking = false)

      i += 1
    }
    info("prevDepthCount: " + mapGraph.vertices.count().toString)
    mapGraph.vertices collect() foreach println
    val minDepthBc = sc.broadcast(minDepth)
    val maxDepthBc = sc.broadcast(maxDepth)
    val vertices = mapGraph.vertices
      .filter(f => f._2.nonEmpty && f._2.length >= minDepthBc.value && f._2.length <= maxDepthBc.value).map {
      case (vertexId, seq) =>
        val min = seq.min
        val until = seq.indexOf(min)
        if(until != 0){
          val leftSlice = seq.slice(0, until)
          val rightSlice = seq.slice(until, seq.length)

          rightSlice ++ leftSlice :+ min
        } else {
          seq :+ min
        }
    }.map(_.reverse).distinct()
    info("Circle Count: " + vertices.count().toString)
    vertices.collect  foreach println
    info("circle result list: ")
    vertices.collect() foreach(f => info(f.toString()))
  //  vertices.map(_.toString()).saveAsTextFile(outPath)

   val subEdges = subGraph.edges.map(edge => SimpleEdge(edge.srcId, edge.dstId, null)).toDF()
    println("subEdges:")
    subGraph.edges.collect foreach println
    Builder.build(subEdges)
  }

}
case class SimpleEdge(srcId: Long, dstId: Long, attr: String) extends Serializable

/*class CycleGraph(id: String, name: String, log: Logger)
  extends MultiTransComponent[util.Collection[SparkData], SparkData](id, name, log) {

  override def apply(inputT: util.Collection[SparkData]): SparkData = {
    val iteratorDF = inputT.iterator()
    val verticesDF = iteratorDF.next().getRawData
    val edgesDF = iteratorDF.next().getRawData

    val sqlContext = verticesDF.sqlContext
    val sc = sqlContext.sparkContext

    import sqlContext.implicits._

    val vertices = verticesDF.map(row => (row.get(0).toString.toLong, row.get(1).toString))
    val edges = edgesDF.map(
      row => Edge(row.get(0).toString.toLong, row.get(1).toString.toLong, row.get(2).toString))
    val graph = Graph(vertices, edges)

    var subGraph = graph.subgraph()
    var inDegreesBc: Broadcast[Array[(VertexId, Int)]] = null

    val epred = (edgeTriplet: EdgeTriplet[String, String]) => true
    val vpred = (vertexId: VertexId, attr: String) => inDegreesBc.value.exists(_._1 == vertexId)

    while (subGraph.inDegrees.count() != subGraph.vertices.count()) {
      val inDegrees = subGraph.inDegrees.collect()

      if (inDegreesBc != null) {
        inDegreesBc.unpersist()
      }
      inDegreesBc = sc.broadcast(inDegrees)

      subGraph.unpersist()
      subGraph = graph.subgraph(epred, vpred)
    }

    var outDegreesBc: Broadcast[Array[(VertexId, Int)]] = null

    while (subGraph.outDegrees.count() != subGraph.vertices.count()) {
      val outDegrees = subGraph.outDegrees.collect()

      if (outDegreesBc != null) {
        outDegreesBc.unpersist()
      }
      outDegreesBc = sc.broadcast(outDegrees)

      subGraph.unpersist()
      subGraph = graph.subgraph(epred, (vertexId: VertexId, attr: String) =>
        outDegreesBc.value.exists(_._1 == vertexId))
    }

    val rawData = subGraph.edges.map(edge => SimpleEdge(edge.srcId, edge.dstId, edge.attr)).toDF()

    Builder.build(rawData)

  }

}

case class SimpleEdge(srcId: Long, dstId: Long, attr: String) extends Serializable*/


