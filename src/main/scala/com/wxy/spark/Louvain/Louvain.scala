package com.wxy.spark.Louvain

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

/**
  * Provides low level louvain community detection algorithm functions.
  *
  * For details on the sequential algorithm see:  Fast unfolding of communities in large networks, Blondel 2008
  */

object Louvain {

  /**
    * Generates a new graph of type Graph[VertexState,Long] based on an input graph of type.
    * Graph[VD,Long].  The resulting graph can be used for louvain computation.
    *
    */
  def createLouvainGraph[VD: ClassTag](graph: Graph[VD, Long]): Graph[VertexState, Long] = {
    // Create the initial Louvain graph.
    // val nodeWeightMapFunc = (e: EdgeTriplet[VD, Long]) => Iterator((e.srcId, e.attr), (e.dstId, e.attr))

    val nodeWeightMapFunc = (e: EdgeContext[VD, Long, Long]) => {
      e.sendToDst(e.attr)
      e.sendToSrc(e.attr)
    }
    val nodeWeightReduceFunc = (e1: Long, e2: Long) => e1 + e2
    val nodeWeights = graph.aggregateMessages(nodeWeightMapFunc, nodeWeightReduceFunc)

    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = weight
      state.internalWeight = 0L
      state.nodeWeight = weight
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    return louvainGraph
  }


  /**
    * Creates the messages passed between each vertex to convey neighborhood community data.
    */
  //  private def sendMsg(et:EdgeTriplet[VertexState,Long]) = {
  //    val m1 = (et.dstId,Map((et.srcAttr.community,et.srcAttr.communitySigmaTot)->et.attr))
  //    val m2 = (et.srcId,Map((et.dstAttr.community,et.dstAttr.communitySigmaTot)->et.attr))
  //    Iterator(m1, m2)
  //  }
  private def msgFun(triplet: EdgeContext[VertexState, Long, Map[(Long, Long), Long]]) = {
    val m1 = Map((triplet.srcAttr.community, triplet.srcAttr.communitySigmaTot) -> triplet.attr)
    val m2 = Map((triplet.dstAttr.community, triplet.dstAttr.communitySigmaTot) -> triplet.attr)
    triplet.sendToDst(m1)
    triplet.sendToSrc(m2)
  }

  /**
    * Merge neighborhood community data into a single message for each vertex
    */
  private def reduceFun(m1: Map[(Long, Long), Long], m2: Map[(Long, Long), Long]) = {
    val newMap = scala.collection.mutable.HashMap[(Long, Long), Long]()
    m1.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }

  /**
    * Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
    * Returns a new set of vertices with the updated vertex state.
    */
  private def louvainVertJoin(louvainGraph: Graph[VertexState, Long], msgRDD: VertexRDD[Map[(Long, Long), Long]], totalEdgeWeight: Broadcast[Long], even: Boolean) = {
    louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      var bestCommunity = vdata.community
      var startingCommunityId = bestCommunity
      var maxDeltaQ = BigDecimal(0.0);
      var bestSigmaTot = 0L
      msgs.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vdata.nodeWeight, vdata.internalWeight, totalEdgeWeight.value)
        //println("   communtiy: "+communityId+" sigma:"+sigmaTotal+" edgeweight:"+communityEdgeWeight+"  q:"+deltaQ)
        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))) {
          maxDeltaQ = deltaQ
          bestCommunity = communityId
          bestSigmaTot = sigmaTotal
        }
      })
      // only allow changes from low to high communties on even cyces and high to low on odd cycles
      if (vdata.community != bestCommunity && ((even && vdata.community > bestCommunity) || (!even && vdata.community < bestCommunity))) {
        //println("  "+vid+" SWITCHED from "+vdata.community+" to "+bestCommunity)
        vdata.community = bestCommunity
        vdata.communitySigmaTot = bestSigmaTot
        vdata.changed = true
      }
      else {
        vdata.changed = false
      }
      vdata
    })
  }

  /**
    * Returns the change in modularity that would result from a vertex moving to a specified community.
    */
  private def q(currCommunityId: Long, testCommunityId: Long, testSigmaTot: Long, edgeWeightInCommunity: Long, nodeWeight: Long, internalWeight: Long, totalEdgeWeight: Long): BigDecimal = {
    val isCurrentCommunity = (currCommunityId.equals(testCommunityId));
    val M = BigDecimal(totalEdgeWeight);
    val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity;
    val k_i_in = BigDecimal(k_i_in_L);
    val k_i = BigDecimal(nodeWeight + internalWeight);
    val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot);

    var deltaQ = BigDecimal(0.0);
    if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
      deltaQ = k_i_in - (k_i * sigma_tot / M)
      //println(s"      $deltaQ = $k_i_in - ( $k_i * $sigma_tot / $M")
    }
    return deltaQ;
  }

  /**
    * Compress a graph by its communities, aggregate both internal node weights and edge
    * weights within communities.
    */
  def compressGraph(graph: Graph[VertexState, Long], debug: Boolean = true): Graph[VertexState, Long] = {

    // aggregate the edge weights of self loops. edges with both src and dst in the same community.
    // WARNING  can not use graph.mapReduceTriplets because we are mapping to new vertexIds
    val internalEdgeWeights = graph.triplets.flatMap(et => {
      if (et.srcAttr.community == et.dstAttr.community) {
        Iterator((et.srcAttr.community, 2 * et.attr)) // count the weight from both nodes  // count the weight from both nodes
      }
      else Iterator.empty
    }).reduceByKey(_ + _)


    // aggregate the internal weights of all nodes in each community
    var internalWeights = graph.vertices.values.map(vdata => (vdata.community, vdata.internalWeight)).reduceByKey(_ + _)

    // join internal weights and self edges to find new interal weight of each community
    val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({ case (vid, (weight1, weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0L)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = weight1 + weight2
      state.nodeWeight = 0L
      (vid, state)
    }).cache()


    // translate each vertex edge to a community edge
    val edges = graph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) Iterator(new Edge(src, dst, et.attr))
      else Iterator.empty
    }).cache()


    // generate a new graph where each community of the previous
    // graph is now represented as a single vertex
    val compressedGraph = Graph(newVerts, edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    // calculate the weighted degree of each node
    // val nodeWeightMapFunc = (e: EdgeTriplet[VertexState, Long]) => Iterator((e.srcId, e.attr), (e.dstId, e.attr))
    // val nodeWeightReduceFunc = (e1: Long, e2: Long) => e1 + e2

    val nodeWeightMsgFunc = (t: EdgeContext[VertexState, Long, Long]) => {
      t.sendToSrc(t.attr)
      t.sendToDst(t.attr)
    }
    val nodeWeightReduceFunc = (e1: Long, e2: Long) => e1 + e2
    val nodeWeights = compressedGraph.aggregateMessages(nodeWeightMsgFunc, nodeWeightReduceFunc)

    // fill in the weighted degree of each node
    // val louvainGraph = compressedGraph.joinVertices(nodeWeights)((vid,data,weight)=> {
    val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      data.communitySigmaTot = weight + data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph

    newVerts.unpersist(blocking = false)
    edges.unpersist(blocking = false)
    return louvainGraph

  }

  /**
    * For a graph of type Graph[VertexState,Long] label each vertex with a community to maximize global modularity.
    * (without compressing the graph)
    */
  def louvain(sc: SparkContext, graph: Graph[VertexState, Long], minProgress: Int = 1, progressCounter: Int = 1): (Double, Graph[VertexState, Long], Int) = {
    var louvainGraph = graph.cache()
    var graphWeight = louvainGraph.vertices.values.map(vdata => vdata.internalWeight + vdata.nodeWeight).reduce(_ + _)
    var totalGraphWeight = sc.broadcast(graphWeight)
    println("totalEdgeWeight: " + totalGraphWeight.value)

    // gather community information from each vertex's local neighborhood
    var msgRDD = louvainGraph.aggregateMessages(msgFun, reduceFun)
    var activeMessages = msgRDD.count() //materializes the msgRDD and caches it in memory

    var updated = 0L - minProgress
    var even = false
    var count = 0
    val maxIter = 100000
    var stop = 0
    var updatedLastPhase = 0L
    do {
      count += 1
      even = !even

      // label each vertex with its best community based on neighboring community information
      val labeledVerts = louvainVertJoin(louvainGraph, msgRDD, totalGraphWeight, even).cache()

      // calculate new sigma total value for each community (total weight of each community)
      val communtiyUpdate = labeledVerts
        .map({ case (vid, vdata) => (vdata.community, vdata.nodeWeight + vdata.internalWeight) })
        .reduceByKey(_ + _).cache()

      // map each vertex ID to its updated community information
      val communityMapping = labeledVerts
        .map({ case (vid, vdata) => (vdata.community, vid) })
        .join(communtiyUpdate)
        .map({ case (community, (vid, sigmaTot)) => (vid, (community, sigmaTot)) })
        .cache()

      // join the community labeled vertices with the updated community info
      val updatedVerts = labeledVerts.join(communityMapping).map({ case (vid, (vdata, communityTuple)) =>
        vdata.community = communityTuple._1
        vdata.communitySigmaTot = communityTuple._2
        (vid, vdata)
      }).cache()
      updatedVerts.count()
      labeledVerts.unpersist(blocking = false)
      communtiyUpdate.unpersist(blocking = false)
      communityMapping.unpersist(blocking = false)

      val prevG = louvainGraph
      louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
      louvainGraph.cache()

      // gather community information from each vertex's local neighborhood
      val oldMsgs = msgRDD
      msgRDD = louvainGraph.aggregateMessages(msgFun, reduceFun).cache()
      activeMessages = msgRDD.count() // materializes the graph by forcing computation

      oldMsgs.unpersist(blocking = false)
      updatedVerts.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)

      // half of the communites can swtich on even cycles
      // and the other half on odd cycles (to prevent deadlocks)
      // so we only want to look for progess on odd cycles (after all vertcies have had a chance to move)
      if (even) updated = 0
      updated = updated + louvainGraph.vertices.filter(_._2.changed).count
      if (!even) {
        println("  # vertices moved: " + java.text.NumberFormat.getInstance().format(updated))
        if (updated >= updatedLastPhase - minProgress) stop += 1
        updatedLastPhase = updated
      }


    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIter)))
    println("\nCompleted in " + count + " cycles")


    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      // sum the nodes internal weight and all of its edges that are in its community
      val community = vdata.community
      var k_i_in = vdata.internalWeight
      var sigmaTot = vdata.communitySigmaTot.toDouble
      msgs.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        if (vdata.community == communityId) k_i_in += communityEdgeWeight
      })
      val M = totalGraphWeight.value
      val k_i = vdata.nodeWeight + vdata.internalWeight
      var q = (k_i_in.toDouble / M) - ((sigmaTot * k_i) / math.pow(M, 2))
      //println(s"vid: $vid community: $community $q = ($k_i_in / $M) -  ( ($sigmaTot * $k_i) / math.pow($M, 2) )")
      if (q < 0) 0 else q
    })
    val actualQ = newVerts.values.reduce(_ + _)

    // return the modularity value of the graph along with the
    // graph. vertices are labeled with their community
    return (actualQ, louvainGraph, count / 2)

  }


  def run(sc: SparkContext, edgesFile: String, vertexFile: String, numPartitions: Int, separator: String) = {

    // read the input into a distributed edge list

    var edgeRDD = sc.textFile(edgesFile).map(row => {
      val tokens = row.split(separator).map(_.trim())
      tokens.length match {
        case 2 => {
          new Edge(tokens(0).toLong, tokens(1).toLong, 1L)
        }
        case 3 => {
          new Edge(tokens(0).toLong, tokens(1).toLong, tokens(2).toLong)
        }
        case _ => {
          throw new IllegalArgumentException("invalid input line: " + row)
        }
      }
    })

    val graph = Graph.fromEdges(edgeRDD, None)
    var louvainGraph = Louvain.createLouvainGraph(graph)

    var level = -1 // number of times the graph has been compressed
    var q = -1.0 // current modularity value

    var halt = false
    do {
      level += 1
      println(s"Starting Louvain level $level")

      // label each vertex with its best community choice at this level of compression
      val (currentQ, currentGraph, passes) = Louvain.louvain(sc, louvainGraph)
      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph

      // If modularity was increased by at least 0.001 compress the graph and repeat
      // halt immediately if the community labeling took less than 3 passes
      //println(s"if ($passes > 2 && $currentQ > $q + 0.001 )")
      if (passes > 2 && currentQ > q + 0.001) {
        q = currentQ
        louvainGraph = Louvain.compressGraph(louvainGraph)
      }
      else {
        halt = true
      }

    } while (!halt)
    q
  }


}
