package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData}
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent
import org.slf4j.Logger

class GraphXComponent(id: String, name: String, log: Logger)
  extends TransformableComponent[SparkData, SparkData](id, name, log) with Configureable {

  var sourceIdField: String = _
  var targetIdField: String = _
  var attrIdField: String = _

  var minDepth: Int = _
  var maxDepth: Int = _
  var maxIterations: Int = _

  override def configure(componentProps: ComponentProps): Unit = {
    sourceIdField = componentProps.getString("sourceIdField", "sourceId")
    targetIdField = componentProps.getString("targetIdField", "targetId")
    attrIdField = componentProps.getString("attrIdField", "attrId")

    minDepth = componentProps.getInt("minDepth", 3)
    maxDepth = componentProps.getInt("maxDepth", 5)
    maxIterations = componentProps.getInt("maxIterations", maxDepth + 1)
  }

  override def apply(inputT: SparkData): SparkData = {
    val df = inputT.getRawData
    val sqlContext = df.sqlContext

    val initialRDD = df.selectExpr(
      "crc32(" + sourceIdField + "), crc32(" + targetIdField + "), " + attrIdField + ", " +
      sourceIdField + ", " + targetIdField)
    val initialVertices = df
      .flatMap(row => Seq(row.get(0).toString.toLong, row.get(1).toString.toLong))
      .distinct().map(vertexId => (vertexId, false))
    val initialEdges = df.map(row => Edge(row.get(0).toString.toLong, row.get(1).toString.toLong,
      row.get(2).toString.toDouble))
    val graph = Graph(initialVertices, initialEdges, false)

    var joinDegrees = graph.inDegrees.fullOuterJoin(graph.outDegrees).filter {
      case (vertexId, (inDegree, outDegree)) => inDegree.getOrElse(0) == 0 ||
                                                outDegree.getOrElse(0) == 0
    }
    var joinCount = joinDegrees.count()
    var joinGraph = graph
    var subGraph = graph
    println("joinCount: " + joinCount)
    while (joinCount > 0) {
      val prevJoinGraph = joinGraph
      joinGraph = subGraph
        .outerJoinVertices(joinDegrees)((vertexId, attr, option) => option.isEmpty)

      val prevSubGraph = subGraph
      subGraph = joinGraph.subgraph(epred => true, (vertexId, attr) => attr).cache()

      val prevJoinDegrees = joinDegrees
      joinDegrees = subGraph.inDegrees.fullOuterJoin(subGraph.outDegrees).filter {
        case (vertexId, (inDegree, outDegree)) => inDegree.getOrElse(0) == 0 ||
                                                  outDegree.getOrElse(0) == 0
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
    println("messagesCount: " + messagesCount)
    while (messagesCount > 0 && i < maxIterations) {

      val prevMessages = messages
      messages = mapGraph.aggregateMessages[Seq[VertexId]](
        ctx => if (ctx.dstAttr.isEmpty || !ctx.dstAttr.contains(ctx.dstId)) {
          ctx
            .sendToDst(ctx.srcId +: ctx.srcAttr)
        },
        (srcAttr, dstAttr) => (srcAttr ++ dstAttr).distinct
      ).cache()

      messagesCount = messages.count()

      val prevGraph = mapGraph
      mapGraph = mapGraph
        .outerJoinVertices(messages)((vertexId, left, right) => right.getOrElse(left)).cache()

      // 释放之前的资源
      prevMessages.unpersist(blocking = false)
      prevGraph.unpersist(blocking = false)

      i += 1
    }
    println("prevDepthCount: " + mapGraph.vertices.count())
    val rawData = mapGraph.vertices
      .filter(f => f._2.nonEmpty && f._2.length >= 3 && f._2.length <= 5)
      .map(f => CaseClass(f._1, f._2))
      .toDF

    println("postDepthCount: " + rawData.count())
    Builder.build(rawData)

  }

}

case class CaseClass(sourceId: Long, targetIds: Seq[Long])
