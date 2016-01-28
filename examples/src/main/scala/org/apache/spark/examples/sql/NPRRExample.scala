package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sctu on 1/25/16.
 */
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

    val edges = sc.textFile("examples/src/main/resources/facebook.txt").map(line => {
      val parts = line.split(" ")
      Edge(parts(0).toInt, parts(1).toInt)
    })
    val df = sqlContext.createDataFrame(edges)
    val data:List[(Int, Int)] = edges.collect().map(e => (e.src, e.dst)).toList

    val result = df.select("src").map(a => {
      data.filter(x_val => x_val._1 == a.getInt(0)).unzip._2.toSet.intersect(data.unzip._1.toSet).flatMap(b => {
        val c = data.filter(x_val => x_val._1 == b).unzip._2.toSet.intersect(data.filter(x_val => x_val._1 == a.getInt(0)).unzip._2.toSet)
        c.map(c_val => s"""${a.getInt(0)},${b},${c_val}""")
      })
    })

    time {
      result.collect()
    }
  }
}
