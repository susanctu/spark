package org.apache.spark.examples.sql

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Relation {
  def fromFile(filename:String): Relation = {
    //println("start of fromFile")
    var arr:Array[ArrayBuffer[Int]] = null
    val handle = Source.fromFile(filename)
    assert(handle != null)
    for (line <- handle.getLines) {
      //println(line)
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
    return fromArrays(arr.map(_.toArray))
  }

  def main(args:Array[String]): Unit = {
    val R = fromFile(args(0))
    val S = R.copy()
    val T = R.copy()
    val triangles = BenchmarkUtil.time { R.firstCol.intersect(T.firstCol).map(a => {
      R.firstCol.getNextCol(a).intersect(S.firstCol).map(b => {
        S.firstCol.getNextCol(b).intersect(T.firstCol.getNextCol(a)).size
      }).sum
    }).sum }
    println(s"""Counted ${triangles} triangles""")
  }

  def fromArrays(arr:Array[Array[Int]]): Relation = {
    new Relation(arr)
  }
}

case class Relation(relation:Array[Array[Int]]) {
  val relationReversed = relation.reverse
  val columns = relationReversed.tail.foldLeft(
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

object Column {
  /**
   * @return whether we reached the end or not
   */
  private def advanceIndex(indices:Array[Int], column:Column, key:Int) : Boolean = {
    val col = column.col
    val currVal:Int = col(indices(key))
    while (col(indices(key)) == currVal) {
      indices(key) += 1
      if (indices(key) == column.end) {
        return true
      }
    }
    return false
  }

  private def advanceToLowerBound(indices:Array[Int], column:Column, key:Int,lowerBound:Int): Boolean = {
    val col = column.col
    val currVal:Int = col(indices(key))
    while (col(indices(key)) < lowerBound) {
      indices(key) += 1
      if (indices(key) == column.end) {
        return true
      }
    }
    return false
  }

  /**
   * This basically runs until we make a single write to the output array
   * @param indices
   * @param sorted
   * @param _minKey
   * @param newColArray
   * @param newColIndex
   * @return
   */
  private def leapfrog(indices:Array[Int],
                       sorted:Seq[Column],
                       _minKey:Int,
                       newColArray:Array[Int],
                       newColIndex:Int) : Option[(Int,Int)] = {
    var maxKey = Math.floorMod(_minKey - 1, sorted.size)
    var minKey = _minKey
    var writeIndex = newColIndex
    while (true) {
      val minCol = sorted(minKey).col
      val maxCol = sorted(maxKey).col
      if (minCol(indices(minKey)) == maxCol(indices(maxKey))) {
        newColArray(writeIndex) = minCol(indices(minKey))
        writeIndex += 1
        return Some((minKey, writeIndex))
      } else {
        if (advanceToLowerBound(indices, sorted(minKey), minKey, maxCol(indices(maxKey)))) {
          return None
        } else {
          maxKey = minKey
          minKey = (minKey + 1) % sorted.size
        }
      }
    }
    assert(false)
    return None
  }

  def intersect(cols:Column*): Column = {
    val sorted = cols.sortBy(c => c.col(c.begin))
    val indices = new Array[Int](sorted.size)
    for (i <- (0 until sorted.size)) {
      indices(i) = sorted(i).begin
    }
    val newColArray = new Array[Int](cols.map(c => c.end - c.begin).min)
    var newColIndex = 0
    var minKey = 0
    while (true) {
      leapfrog(indices, sorted, minKey, newColArray, newColIndex) match {
        case Some((newMinKey, writeIndex)) => {
          if (advanceIndex(indices,sorted(newMinKey), newMinKey)) {
            return new Column(newColArray, 0, writeIndex, None)
          }
          minKey = (newMinKey + 1) % sorted.size
          newColIndex = writeIndex
        }
        case None => {
          return new Column(newColArray, 0, newColIndex, None)
        }
      }
    }
    assert(false)
    return new Column(null, 0, 0, None)
  }
}

class Column(val col:Array[Int], val begin:Int, val end:Int, val nextCol:Option[Column]) extends Serializable with Iterable[Int] {

  def intersect(other: Column): Column = {
    Column.intersect(this, other)
  }

  private def lowerBound(list: Array[Int], start:Int, end: Int, key:Int): Int = {
    val mid = start/2 + end/2
    if (end == start) return -1;
    if (list(mid) == key && (mid == 0 || list(mid-1) < key)) {
      return mid
    } else if (list(mid) >= key) {
      lowerBound(list, start, mid, key)
    } else {
      lowerBound(list, mid + 1, end, key)
    }
  }
  def getNextCol(v:Int): Column = {
    val beginForV = lowerBound(col, begin, end, v)
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

