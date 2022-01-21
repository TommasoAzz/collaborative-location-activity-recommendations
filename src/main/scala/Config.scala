package it.unibo.clar

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object Config {
  val DISTANCE_THRESHOLD = 100 // meters
  val TIME_THRESHOLD = 300 // seconds
  var DEFAULT_PARALLELISM = 1 // number of partitions
  val STAY_REGION_SIDE_LENGTH = 600.0 // length of the side of the stay region square
  val GRID_CELL_SIDE_LENGTH: Double = STAY_REGION_SIDE_LENGTH / 3
  val WORLD_BOTTOM_LEFT_LONGITUDE: Double = -90.0
  val WORLD_BOTTOM_LEFT_LATITUDE: Double = -180.0

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
