package org.apache.spark.examples.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sctu on 1/27/16.
 */
object BarbellExample {
  def  main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.FATAL);
    val conf = new SparkConf().setAppName("BarbellGHDExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val edges = sc.textFile(args(0)).map(line => {
      val parts = line.split(" ")
      Edge(parts(0).toInt, parts(1).toInt)
    })

    val df = sqlContext.createDataFrame(edges)
    val data:List[(Int, Int)] = edges.collect().map(e => (e.src, e.dst)).toList

    val bag1 = df.select("src").distinct.map(a => {
      val count = data.filter(x_val => x_val._1 == a.getInt(0)).unzip._2.toSet.intersect(data.unzip._1.toSet).toList.map(b => {
        val c = data.filter(x_val => x_val._1 == b).unzip._2.toSet.intersect(data.filter(x_val => x_val._1 == a.getInt(0)).unzip._2.toSet)
        c.size
      }).sum
      (a.getInt(0), count)
    })

    // kind of redundant, but perhaps a more fair comparison
    val bag2 = df.select("src").distinct.map(a => {
      val count = data.filter(x_val => x_val._1 == a.getInt(0)).unzip._2.toSet.intersect(data.unzip._1.toSet).toList.map(b => {
        val c = data.filter(x_val => x_val._1 == b).unzip._2.toSet.intersect(data.filter(x_val => x_val._1 == a.getInt(0)).unzip._2.toSet)
        c.size
      }).sum
      (a.getInt(0), count)
    })

    val bmark = new Benchmarker()
    val bag1df = bmark.time { sqlContext.createDataFrame(bag1) }
    val bag1collected = bmark.time { bag1.collect }
    val bag1r2a = bmark.time { bag1collected.toMap}
    val bag2df = bmark.time { sqlContext.createDataFrame(bag2) }
    val bag2collected = bmark.time { bag2.collect }
    val bag2r2a = bmark.time { bag2collected.toMap}

    val topBag = df.select("src").intersect(bag1df.select("_1")).map(a => {
      data.filter(x_val => x_val._1 == a.getInt(0)).unzip._2.intersect(bag2collected.unzip._1).map(b_val => {
        bag1r2a.get(a.getInt(0)).get * bag2r2a.get(b_val).get
      }).sum
    })

    bmark.time {
      println(topBag.sum)
    }

    bmark.printTotalElapsed()
  }
}
