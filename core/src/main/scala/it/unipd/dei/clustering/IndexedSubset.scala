package it.unipd.dei.clustering

/**
  * A mutable view of a subset of a set represented by and indexed seq.
  */
class IndexedSubset[T] private (val superSet: IndexedSeq[T], private val flags: Array[Boolean]) {

  def add(idx: Int): Unit = flags(idx) = true

  def remove(idx: Int): Unit = flags(idx) = false

  def contains(idx: Int): Boolean = flags(idx)

  def get(idx: Int): Option[T] =
    if(contains(idx)) {
      Some(superSet(idx))
    } else {
      None
    }

  private def traversableLike = superSet.zipWithIndex.filter({case (e, i) => flags(i)}).map(_._1)

  def supersetIndices: Iterator[Int] =
    superSet.view.zipWithIndex.filter({case (e, i) => flags(i)}).map(_._2).iterator

  def toSet: Set[T] = traversableLike.toSet

  def toVector: Vector[T] = traversableLike.toVector

  def copy(): IndexedSubset[T] = new IndexedSubset[T](superSet, flags.clone())

  def clear(): Unit = {
    var i = 0
    while (i < flags.length) {
      flags(i) = false
      i += 1
    }
  }

  def size: Int = {
    var s = 0
    var i = 0
    while (i < flags.length) {
      if (flags(i)) {
        s += 1
      }
      i += 1
    }
    s
  }

}

object IndexedSubset {

  def apply[T](superSet: IndexedSeq[T]) = new IndexedSubset[T](superSet, Array.ofDim(superSet.size))

}