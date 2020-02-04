package com.wxy.spark.Louvain

import java.util.{Date, Properties}

import org.apache.spark.{SparkConf, SparkContext}

object Main extends {

  def main(args: Array[String]): Unit = {
    val confSparkMaster = "local"

    val vertexFile = "src/data/vertex.graph"
    val edgesFile = "src/data/edge.graph"
    val numPartitions = 1

    val startTime = new Date().getTime
    val sc = new SparkContext(confSparkMaster, "Spark Graphx Louvain")

    val q = Louvain.run(sc, edgesFile, vertexFile, numPartitions, "\t")

    val endTime = new Date().getTime
    val ss = (endTime - startTime) / (1000)
    println("Runtime " + ss + "s")


    println("Q => " + q.toString)
    sc.stop()
  }
}
