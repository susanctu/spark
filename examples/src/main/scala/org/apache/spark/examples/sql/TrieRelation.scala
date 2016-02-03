package org.apache.spark.examples.sql

import scala.collection.mutable.ArrayBuffer

object TrieRelation {
  def fromFile(filename:String): TrieRelation = {
    TrieRelation(ColumnRelation.fromFile(filename).firstCol)
  }

  def fromArrays(arr:Array[Array[Int]]): TrieRelation = {
    TrieRelation(ColumnRelation.fromArrays(arr).firstCol)
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
}

object TrieBuildUtil {
  def makeSortedArrayDistinct(arr:Array[Int], start:Int, end:Int): Array[Int] = {
    val newArr = new ArrayBuffer[Int]
    var haveWritten = false
    for (i <- (start until end)) {
      if (haveWritten) {
        if (newArr.last != arr(i)) {
          newArr += arr(i)
        }
      } else {
        newArr += arr(i)
        haveWritten = true
      }
    }
    newArr.toArray
  }
}
case class TrieRelation(column:Column) extends Relation {
  private val trie = makeTrie(column)
  private def makeTrie(column:Column): TrieBlock = {
    val distinctSorted = TrieBuildUtil.makeSortedArrayDistinct(column.col, column.begin, column.end)
    val children = new Array[TrieBlock](distinctSorted.size)
    val newTrieBlock = new TrieBlock(distinctSorted, children)

    if (column.nextCol.isDefined) {
      for (i <- (0 until distinctSorted.size)) {
        val v = distinctSorted(i)
        children(i) = makeTrie(column.getNextCol(v))
      }
    }
    return newTrieBlock
  }
  override def firstCol: Block = { trie }
}

class TrieBlock(override val col:Array[Int], val children:Array[TrieBlock]) extends Serializable with Block {
  override val begin = 0
  override val end = col.size
  override def getNextCol(v: Int): Block = {
    val index = lowerBound(0, col.size, v)
    children(index)
  }

  override def intersect(other: Block): Block = {
    IntersectUtil.intersect(this, other)
  }

  override def iterator: Iterator[Int] = {
    return new TrieBlockIterator
  }

  class TrieBlockIterator extends Iterator[Int] {
    var index = 0
    def hasNext = index < col.size
    def next() = {
      val retValue = col(index)
      index += 1
      retValue
    }
  }
}
