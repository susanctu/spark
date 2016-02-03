package org.apache.spark.examples.sql

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ColumnRelation {
  def fromFileToArr(filename:String): Array[Array[Int]] = {
    var arr:Array[ArrayBuffer[Int]] = null
    val handle = Source.fromFile(filename)
    assert(handle != null)
    for (line <- handle.getLines) {
      val parts = line.split("\\s+")
      if (arr == null) {
        arr = new Array[ArrayBuffer[Int]](parts.size)
      }
      for (i <- (0 until parts.size)) {
        if (arr(i) == null) {
          arr(i) = new ArrayBuffer[Int]()
        }
        arr(i) += parts(i).toInt
      }
    }
    arr.map(_.toArray)
  }
  def fromFile(filename:String): ColumnRelation = {
    return fromArrays(fromFileToArr(filename))
  }

  def main(args:Array[String]): Unit = {
    val R = fromFile(args(0))
    val S = R.copy()
    val T = R.copy()
    val triangles = R.firstCol.intersect(T.firstCol).map(a => {
      R.firstCol.getNextCol(a).intersect(S.firstCol).map(b => {
        S.firstCol.getNextCol(b).intersect(T.firstCol.getNextCol(a)).size
      }).sum
    }).sum
    println(s"""Counted ${triangles} triangles""")
  }

  def fromArrays(arr:Array[Array[Int]]): ColumnRelation = {
    new ColumnRelation(arr)
  }
}

case class ColumnRelation(relation:Array[Array[Int]]) extends Relation {
  private val relationReversed = relation.reverse
  private val columns = relationReversed.tail.foldLeft(
    List[Column](new Column(relationReversed.head, 0, relationReversed.head.size, None)
    ))((acc:List[Column], elem:Array[Int]) => {
    val newCol = new Column(elem, 0, elem.size, Some(acc.head))
    newCol::acc
  })
  def firstCol: Column = {
    return columns.head
  }
}

class ColumnNotFoundError(value:Int) extends Exception("Did not find value " + value)

class Column(val col:Array[Int], override val begin:Int, override val end:Int, val nextCol:Option[Column]) extends Serializable with Block {

  def intersect(other: Block): Block = {
    IntersectUtil.intersect(this, other)
  }

  def getNextCol(v:Int): Column = {
    val beginForV = lowerBound(begin, end, v)
    if (beginForV < 0) {
      throw new ColumnNotFoundError(v)
    }
    val endForV = beginForV + (beginForV until end).toStream.takeWhile(col(_) == v).size
    if (nextCol.isEmpty) {
      throw new ColumnNotFoundError(v)
    } else {
      val nextColExistent = nextCol.get
      return new Column(nextColExistent.col, beginForV, endForV, nextColExistent.nextCol)
    }
  }

  override def iterator: Iterator[Int] = {
    return new ColumnIterator
  }

  class ColumnIterator extends Iterator[Int] {
    var index = begin
    def hasNext = index < end
    def next() = {
      val retValue = col(index)
      index += 1
      retValue
    }
  }
}
