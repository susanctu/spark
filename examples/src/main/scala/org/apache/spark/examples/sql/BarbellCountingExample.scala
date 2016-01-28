package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


object BarbellCountingExample {
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
      """select count(*)
        | from graph as t1, graph as t2, graph as t3, graph as t4, graph as t5, graph as t6, graph as t7
        | where
        | t1.dst = t2.src
        | and t2.dst = t3.dst
        | and t1.src = t3.src
        | and t4.dst = t5.src
        | and t5.dst = t6.dst
        | and t4.src = t6.src
        | and t1.src = t4.src""".stripMargin

    val b = new Benchmarker
    val res = sqlContext.sql(triangle)
    b.time { res.collect() }
  }
}