package org.apache.spark.examples.graphx

import org.apache.spark.examples.sql.BenchmarkUtil
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.{PartitionStrategy, GraphLoader}

/**
 * Created by sctu on 1/28/16.
 */
object GraphXTriangleCount {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("TriangleCountingExample")
    val sc = new SparkContext(conf)
    // Load the edges in canonical order and partition the graph for triangle count
    val graph = GraphLoader.edgeListFile(sc, args(0), true).partitionBy(PartitionStrategy.RandomVertexCut)
    graph.cache()
    // Find the triangle count for each vertex
    val accum1 = sc.accumulator(0, "My Accumulator")
    val accum2 = sc.accumulator(0, "My Accumulator")
    BenchmarkUtil.time { graph.triangleCount().vertices.foreach(v => accum1 += v._2) }
    val  triCounts = BenchmarkUtil.time { graph.triangleCount().vertices.foreach(v => accum2 += v._2) }
    println(accum2.value)
  }
}
