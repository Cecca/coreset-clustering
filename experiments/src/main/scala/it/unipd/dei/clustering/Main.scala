package it.unipd.dei.clustering

import org.rogach.scallop.ScallopConf

object Main {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input = opt[String](required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {

  }

}
