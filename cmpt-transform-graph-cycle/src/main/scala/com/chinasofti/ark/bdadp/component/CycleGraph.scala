package com.chinasofti.ark.bdadp.component

import java.util

import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.MultiTransComponent
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.slf4j.Logger

class CycleGraph(id: String, name: String, log: Logger)
  extends MultiTransComponent[util.Collection[SparkData], SparkData](id, name, log) {

  override def apply(inputT: util.Collection[SparkData]): SparkData = {
    val iteratorDF = inputT.iterator()
    val verticesDF = iteratorDF.next().getRawData
    val edgesDF = iteratorDF.next().getRawData

    val sqlContext = verticesDF.sqlContext
    val sc = sqlContext.sparkContext

    val vertices = verticesDF.map(row => (row.getString(0).toLong, row.getString(1)))
    val edges = edgesDF.map(row => Edge(row.getString(0).toLong, row.getString(1).toLong, row.getString(2)))
    val graph = Graph(vertices, edges)

    var subGraph = graph.subgraph()
    var inDegreesBc: Broadcast[Array[(VertexId, Int)]] = null

    def vpred = (vertexId: VertexId, attr: String) =>
      inDegreesBc.value.exists(_._1 == vertexId)

    while (subGraph.inDegrees.count() != subGraph.vertices.count()) {
      val inDegrees = subGraph.inDegrees.collect()
      inDegreesBc = sc.broadcast(inDegrees)

      subGraph = graph.subgraph((epred) => true, vpred)
    }

    import sqlContext.implicits._

    val rawData = subGraph.edges.map(edge => SimpleEdge(edge.srcId, edge.dstId, edge.attr)).toDF()

    Builder.build(rawData)

  }

}

case class SimpleEdge(srcId: Long, dstId: Long, value: String)



