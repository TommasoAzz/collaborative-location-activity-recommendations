package it.unibo

import org.apache.spark.SparkContext

package object clar {
  def time[R](label: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println(s"$label - elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }


}
