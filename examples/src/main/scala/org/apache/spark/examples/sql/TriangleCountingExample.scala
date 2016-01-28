package org.apache.spark.examples.sql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

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

    val edges = sc.textFile(args(0)).map(line => {
      val parts = line.split(" ")
      Edge(parts(0).toInt, parts(1).toInt)
    })

    val graph = sqlContext.createDataFrame(edges)
    graph.registerTempTable("graph")
    val triangle =
      """select t1.src, t1.dst, t2.dst
        | from graph as t1, graph as t2, graph as t3
        | where
        | t1.src = t2.src
        | and t1.dst = t3.src
        | and t2.dst = t3.dst""".stripMargin

    val res = sqlContext.sql(triangle)
    val count = time { res.collect() }
    count.foreach(row => println(row))
  }
}
