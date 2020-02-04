package com.wxy.spark.FastCD

import java.util.{Date, Properties}

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object FastCD {

  def getQAndCommunityNumbers(sc: SparkContext, graph: Graph[NodeData, Long]) = {
    val w = sc.broadcast(graph.edges.reduce((x1, x2) => new Edge(x1.srcId, x2.dstId, x1.attr + x2.attr)).attr)
    val Qs = graph.aggregateMessages[(NodeData)](
      { tri =>
        if (tri.srcAttr.communityID == tri.dstAttr.communityID) {
          if (tri.srcAttr.nodeID == tri.srcAttr.communityID) tri.sendToSrc(tri.srcAttr)
          else if (tri.dstAttr.nodeID == tri.dstAttr.communityID)
            tri.sendToDst(tri.dstAttr)
        }
      },
      //  find the nodes that can represent different communities.If the number of message is more than one ,a must equal b
      (a, b) => {
        a
      }
    ).map {
      x => {
        val Sc = x._2.neighCommunityDegreeSum
        val Ic = x._2.communityDegreeSum
        val ID = x._2.communityID
        val q = (Ic - Sc * Sc * 1.0 / (2 * w.value)) / (2 * w.value)
        Tuple2(ID, q)
      }
    }.collect()
    val iter = Qs.iterator
    var sum = 0.0
    var number = 0L
    while (iter.hasNext) {
      var line = iter.next()
      sum += line._2
      number += 1
    }
    Tuple2(sum, number)
  }

  // update the information of nodes
  def updateNodeData(graph: Graph[NodeData, Long]): RDD[(VertexId, NodeData)] = {
    graph.aggregateMessages[(Long, Long, Long, Long, NodeData)](
      //edges attribute,  the communityId of sender, Ic ,Sc,the information of receiver
      t => {
        t.sendToDst(t.attr, t.srcAttr.communityID, 0, 0, t.dstAttr)
        t.sendToSrc(t.attr, t.dstAttr.communityID, 0, 0, t.srcAttr)
      },

      // reduce message if receiver receive more than one message
      (a, b) => {
        var li = b._3 + a._3
        li += a._1
        li += b._1
        var si = 0L
        if (a._2 != a._5.communityID) {
          si += a._1
        }
        if (b._2 != b._5.communityID) {
          si += b._1
        }
        (0L, -1L, li, si, a._5)
      }
    ).map {
      x =>
        var node = new NodeData(x._2._5.nodeID, x._2._5.communityID)
        var li = x._2._3
        var si = x._2._4
        if (li == 0) li = x._2._1
        if (si == 0 && x._2._2 != x._2._5.communityID) si = x._2._1

        node.communityDegreeSum = li
        node.neighCommunityDegreeSum = si
        Tuple2(x._1, node)
    }
  }

  // update the information of edges
  def EdgesReduce(graph: Graph[NodeData, Long]): RDD[Edge[Long]] = {

    graph.triplets.map { f =>
      Edge(f.srcAttr.communityID, f.dstAttr.communityID, (f.attr))
    }.flatMap {
      e => {
        var key = ""
        if (e.srcId > e.dstId) key = e.srcId + "-" + e.dstId
        else key = e.dstId + "-" + e.srcId
        List((key, e.attr))
      }
    }.reduceByKey(_ + _).map {
      e => {
        var src = e._1.split("-")
        Edge(src(0).toLong, src(1).toLong, e._2)
      }
    }
  }

  def distributeCommunity(graph: Graph[NodeData, Long], w: Broadcast[Long]): Graph[NodeData, Long] = {
    var old = graph.vertices
    var newVertexRDD: RDD[(VertexId, NodeData)] = graph.aggregateMessages[(Long, NodeData, NodeData, Double)](
      t => if (t.srcAttr.communityID != t.dstAttr.communityID) {
        t.sendToDst(t.attr, t.srcAttr, t.dstAttr, 0)
        t.sendToSrc(t.attr, t.dstAttr, t.srcAttr, 0)
      },
      (a, b) => {
        var avalue = 0.0
        var bvalue = 0.0
        if (a._1 != 0) avalue = (w.value * a._1 - a._2.neighCommunityDegreeSum * a._3.communityDegreeSum) / (2.0 * w.value * w.value)
        if (b._1 != 0) bvalue = (w.value * b._1 - b._2.neighCommunityDegreeSum * b._3.communityDegreeSum) / (2.0 * w.value * w.value)
        if (avalue <= 0 && bvalue <= 0) (0, null, null, 0)
        else if (avalue > bvalue) (a._1, a._2, a._3, avalue)
        else (b._1, b._2, b._3, bvalue)
      }
    ).filter(x => x._2._1 != 0).map {
      data => {
        var node = new NodeData(data._1, data._2._2.communityID)
        node.communityDegreeSum = data._2._3.communityDegreeSum
        node.Q = data._2._4
        //node.sci=data._2._3.sci
        node.isUpdate = true
        Tuple2(data._1, node)
      }
    }
    newVertexRDD = old.union(newVertexRDD).reduceByKey {
      (x, y) =>
        if (x.isUpdate) {
          x.isUpdate = false
          x
        }
        else {
          y.isUpdate = false
          y
        }

    }
    var tgraph = Graph(newVertexRDD, graph.edges).cache()
    var pregraph = tgraph


    var subRdd: RDD[(VertexId, NodeData)] = tgraph.aggregateMessages[(NodeData)](
      tri => {
        var node: NodeData = null

        if (tri.dstAttr.communityID == tri.srcAttr.nodeID && tri.srcAttr.communityID != tri.srcAttr.nodeID) {
          node = tri.dstAttr
          node.communityID = tri.srcAttr.communityID
          node.isUpdate = true
          tri.sendToDst(node)
        }
        else if (tri.srcAttr.communityID == tri.dstAttr.nodeID && tri.dstAttr.communityID != tri.dstAttr.nodeID) {
          node = tri.srcAttr
          node.communityID = tri.dstAttr.communityID
          node.isUpdate = true
          tri.sendToSrc(node)
        }

      },
      (a, b) => b

    )

    newVertexRDD = newVertexRDD.union(subRdd).reduceByKey {
      (x, y) =>
        if (x.isUpdate) {
          x.isUpdate = false
          x
        }
        else {
          y.isUpdate = false
          y
        }

    }

    tgraph = Graph(newVertexRDD, tgraph.edges).cache()
    pregraph.unpersist(false)
    pregraph = tgraph
    var edges = EdgesReduce(tgraph)
    tgraph = Graph(newVertexRDD, edges).cache()
    pregraph.unpersist(false)
    pregraph = tgraph

    newVertexRDD = updateNodeData(tgraph)

    pregraph.unpersist(false)
    Graph(newVertexRDD, edges)
  }

  def run(sc: SparkContext, edgesFile: String, vertexFile: String, numPartitions: Int, separator: String) = {
    var continue = true

    var edges: RDD[Edge[(Long)]] = sc.textFile(edgesFile, numPartitions).map(row => {
      var record = row.split(separator)
      record.length match {
        case 2 => {
          Edge(record(0).toLong, record(1).toLong, (1L))
        }
        case 3 => {
          Edge(record(0).toLong, record(1).toLong, (record(2).toLong))
        }
        case _ => {
          throw new IllegalArgumentException("invalid input line: " + row)
        }
      }
    })

    // 总权重
    val w = sc.broadcast(edges.reduce((x1, x2) => new Edge(x1.srcId, x2.dstId, x1.attr + x2.attr)).attr)

//    // 初始化： 每个节点为一个独立的社区，社区ID即节点ID
//    var vertices: RDD[(VertexId, NodeData)] = sc.textFile(vertexFile, numPartitions).map(line => {
//      var lineLong = line.toLong
//      var node = new NodeData(lineLong, lineLong)
//      Tuple2(lineLong, node)
//    })
//
//    var graph = Graph(vertices, edges).cache()


    val defaultVertex = new NodeData(0L, 0L)
    val init_graph = Graph.fromEdges(edges, defaultVertex)
    val nodeWeightMapFunc = (e: EdgeContext[NodeData, Long, Long]) => {
      e.sendToDst(e.attr)
      e.sendToSrc(e.attr)
    }
    val nodeWeightReduceFunc = (e1: Long, e2: Long) => e1 + e2
    val nodeWeights = init_graph.aggregateMessages(nodeWeightMapFunc, nodeWeightReduceFunc)

    var graph = init_graph.outerJoinVertices(nodeWeights)((vid, data, _) => {
      var lineLong = vid.toLong
      var node = new NodeData(lineLong, lineLong)
      node
    })


    // the pregraph is used for release the cache of graph
    var pregraph = graph

    // count the number of vertices if the property named Q in NodeData is more than 0
    var newnum: Long = 0L

    var newvertexRDD = updateNodeData(graph)
    graph = Graph(newvertexRDD, edges).cache()

    pregraph.unpersist(false)
    pregraph = graph

    do {
      graph = distributeCommunity(graph, w)
      graph.cache()
      pregraph.unpersist(false)
      pregraph.edges.unpersist(false)
      newnum = graph.vertices.filter(x => x._2.Q > 0).count()
      if (newnum == 0) continue = false
    } while (continue)

    graph
  }


}
