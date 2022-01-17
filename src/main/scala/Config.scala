package it.unibo.clar

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object Config {
  val DISTANCE_THRESHOLD = 100 // meters
  val TIME_THRESHOLD = 300 // seconds
  val LOOP_THRESHOLD = 60 // number of points
  var DEFAULT_PARALLELISM = 1 // number of partitions

  private def _sparkSession: SparkSession = {
    SparkSession.builder
      .master("local[*]") // local[*] Run Spark locally with as many worker threads as logical cores on your machine
      .appName("CollaborativeLocationActivityRecommendations")
      .getOrCreate()
  }

  def sparkSession: SparkSession = {
    val session = _sparkSession

    DEFAULT_PARALLELISM = session.sparkContext.defaultParallelism
    session.sparkContext.setLogLevel("WARN")

    session
  }

  def loadHadoop(): Unit = {
    val hadoopConfig: Configuration = _sparkSession.sparkContext.hadoopConfiguration

    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  }
}
