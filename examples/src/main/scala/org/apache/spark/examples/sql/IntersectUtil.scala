package org.apache.spark.examples.sql

object IntersectUtil {
  /**
   * @return whether we reached the end or not
   */
  private def advanceIndex(indices:Array[Int], column:Block, key:Int) : Boolean = {
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

  private def advanceToLowerBound(indices:Array[Int], column:Block, key:Int,lowerBound:Int): Boolean = {
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
                       sorted:Seq[Block],
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

  def intersect(cols:Block*): Block = {
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
