package org.apache.spark.examples.sql

/**
 * Created by sctu on 1/27/16.
 */

case class Edge(src:Int, dst:Int)

object BenchmarkUtil {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }
}

class Benchmarker {
  private var totalTime:Long = 0

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    totalTime += (t1 - t0)
    result
  }

  def printTotalElapsed(): Unit = {
    println("Total elapsed time: " + totalTime +"ns")
  }

}
