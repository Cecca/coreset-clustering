package it.unipd.dei.clustering

object MemoryUtils {

  val KB: Long = 1024
  val MB: Long = 1024*KB
  val GB: Long = 1024*MB
  val TB: Long = 1024*GB

  def formatBytes(b: Long): String = {
    if (b > TB)      f"${b.toDouble / TB}%.2f TB"
    else if (b > GB) f"${b.toDouble / GB}%.2f GB"
    else if (b > MB) f"${b.toDouble / MB}%.2f MB"
    else if (b > KB) f"${b.toDouble / KB}%.2f KB"
    else             f"$b bytes"
  }

  /** Number of bytes required by a n x n matrix of doubles */
  def matrixBytes(n: Long): Long = n*n*8

  def freeBytes(): Long = {
    val runtime = Runtime.getRuntime
    val allocated = runtime.totalMemory() - runtime.freeMemory()
    runtime.maxMemory() - allocated
  }

}
