package it.unibo.clar

object Config {
  val DISTANCE_THRESHOLD = 100 // meters
  val TIME_THRESHOLD = 300 // seconds
  val LOOP_THRESHOLD = 60 // number of points
  var DEFAULT_PARALLELISM = 1 // number of partitions of an RDD
}
