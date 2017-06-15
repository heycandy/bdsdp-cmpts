package com.chinasofti.ark.bdadp.component

import java.util

import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.MultiTransComponent
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.slf4j.Logger

class CycleGraph(id: String, name: String, log: Logger)
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

case class SimpleEdge(srcId: Long, dstId: Long, attr: String) extends Serializable
