package it.unibo.clar
package config

import org.apache.spark.sql.SparkSession

object SparkProjectConfig {
  var DEFAULT_PARALLELISM = 1 // number of partitions

  private def _sparkSession(master: String): SparkSession = {
    var builder = SparkSession.builder.appName("CollaborativeLocationActivityRecommendations")

    if (master != "default") {
      builder = builder.master(master)
    }

    builder.getOrCreate()
  }

  def sparkSession(master: String, parall: Int): SparkSession = {
    val session = _sparkSession(master)

    DEFAULT_PARALLELISM = parall
    session.sparkContext.setLogLevel("WARN")

    session
  }
}
