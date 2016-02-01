package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sctu on 2/1/16.
 */
object ColumnTriangleNPRRExample {
  def  main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ColumnTriangleNPRRExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val edges = sc.textFile(args(0)).map(line => {
      val parts = line.split(" ")
      Edge(parts(0).toInt, parts(1).toInt)
    })
    val df = sqlContext.createDataFrame(edges)
    val R = Relation.fromFile(args(0))
    val S = R.copy()
    val T = R.copy()

    val result = df.select("src").distinct.map(wrappedA => {
      val a = wrappedA.getInt(0)
      R.firstCol.getNextCol(a).intersect(S.firstCol).map(b => {
        S.firstCol.getNextCol(b).intersect(T.firstCol.getNextCol(a)).size
      }).sum
    })

    val totalCount = BenchmarkUtil.time { result.sum }
    println("counted " + totalCount + " triangles")
  }
}
