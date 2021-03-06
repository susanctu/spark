package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


object BarbellCountingExample {
  def  main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TriangleCountingExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val edges = sc.textFile(args(0)).map(line => {
      val parts = line.split(" ")
      Edge(parts(0).toInt, parts(1).toInt)
    })

    val graph = sqlContext.createDataFrame(edges)
    graph.registerTempTable("graph")
    val triangle =
      """select count(*)
        | from graph as t1, graph as t2, graph as t3, graph as t4, graph as t5, graph as t6, graph as t7
        | where
        | t1.src = t2.src
        | and t1.dst = t3.src
        | and t2.dst = t3.dst
        | and t4.src = t5.src
        | and t4.dst = t6.src
        | and t5.dst = t6.dst
        | and t7.src = t1.src
        | and t7.dst = t4.src""".stripMargin

    val b = new Benchmarker
    val res = sqlContext.sql(triangle)
    val count = b.time { res.collect() }
    println(count(0))
  }
}