package org.apache.spark.examples.sql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
 * Created by sctu on 1/26/16.
 */


object TriangleCountingExample {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def  main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TriangleCountingExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val edges = sc.textFile("examples/src/main/resources/facebook.txt").map(line => {
      val parts = line.split(" ")
      Edge(parts(0).toInt, parts(1).toInt)
    })

    val graph = sqlContext.createDataFrame(edges)
    graph.registerTempTable("graph")
    val triangle =
      """select t1.src,t2.src,t2.dst
        | from graph as t1, graph as t2, graph as t3
        | where
        | t1.dst = t2.src
        | and t2.dst = t3.dst
        | and t1.src = t3.src""".stripMargin

    val res = sqlContext.sql(triangle)
    time { res.collect() }
  }
}
