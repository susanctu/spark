package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object NPRRExample {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def  main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NPRRExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val edges = sc.textFile(args(0)).map(line => {
      val parts = line.split(" ")
      Edge(parts(0).toInt, parts(1).toInt)
    })
    val df = sqlContext.createDataFrame(edges)
    val data = sc.broadcast(edges.collect().map(e => (e.src, e.dst)).toList)

    val result = df.select("src").distinct.map(a => {
      data.value.filter(x_val => x_val._1 == a.getInt(0)).unzip._2.toSet.intersect(data.value.unzip._1.toSet).toList.map(b => {
        val c = data.value.filter(x_val => x_val._1 == b).unzip._2.toSet.intersect(data.value.filter(x_val => x_val._1 == a.getInt(0)).unzip._2.toSet)
        c.size
      }).foldLeft(0)(_ + _)
    })

    val accum = sc.accumulator(0, "My Accumulator")
    val count = time {
      result.foreach(c => accum += c)
      println(accum.value)
    }
  }
}
