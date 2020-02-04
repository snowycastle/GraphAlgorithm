package com.wxy.spark.FastCD

import java.util.{Date, Properties}

import org.apache.spark.{SparkConf, SparkContext}

object Main extends {

  def main(args: Array[String]): Unit = {

    //    val props = new Properties()
    //    props.load(this.getClass.getResourceAsStream("/application.properties"))

    //    // 验证输入参数
    //    if (args.length < 3) {
    //      println("Param:<verticeFile> <edgesFile> <patition>")
    //      System.exit(0)
    //    }

    val confSparkMaster = "local"

    val vertexFile = "src/data/vertex.graph"
    val edgesFile = "src/data/edge.graph"
    val numPartitions = 1

    val startTime = new Date().getTime
    val sc = new SparkContext(confSparkMaster, "Spark Graphx FastCD")

    val graph = FastCD.run(sc, edgesFile, vertexFile, numPartitions,"\t")

    val endTime = new Date().getTime
    val ss = (endTime - startTime) / (1000)
    println("Runtime " + ss + "s")

    val data = FastCD.getQAndCommunityNumbers(sc, graph)
    println("Q => " + data._1 + "\nThe number of community is " + data._2)
    sc.stop()

  }
}



