package org.apache.spark.examples.sql

trait Relation {
  def firstCol: Block
}

trait Block extends Iterable[Int] {
  def col:Array[Int]
  def begin:Int
  def end:Int
  def getNextCol(v:Int): Block
  def intersect(other: Block): Block
  def lowerBound(start:Int, end: Int, key:Int): Int = {
    val mid = start/2 + end/2
    if (end == start) return -1;
    if (col(mid) == key && (mid == 0 || col(mid-1) < key)) {
      return mid
    } else if (col(mid) >= key) {
      lowerBound(start, mid, key)
    } else {
      lowerBound(mid + 1, end, key)
    }
  }
}
