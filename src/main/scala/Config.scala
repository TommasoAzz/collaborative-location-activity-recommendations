package it.unibo.clar

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object Config {
  val DISTANCE_THRESHOLD = 100 // meters
  val TIME_THRESHOLD = 300 // seconds
  val LOOP_THRESHOLD = 60 // number of points
  var DEFAULT_PARALLELISM = 1 // number of partitions

  private def _sparkSession(master: String): SparkSession = {
    var builder = SparkSession.builder.appName("CollaborativeLocationActivityRecommendations")

    if(master != "default") {
      builder = builder.master(master)
    }

    builder.getOrCreate()
  }

  def sparkSession(master: String): SparkSession = {
    val session = _sparkSession(master)

    DEFAULT_PARALLELISM = session.sparkContext.defaultParallelism
    session.sparkContext.setLogLevel("WARN")

    session
  }

  def loadHadoop(): Unit = {
    val hadoopConfig: Configuration = SparkSession.builder.getOrCreate().sparkContext.hadoopConfiguration

    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  }
}
