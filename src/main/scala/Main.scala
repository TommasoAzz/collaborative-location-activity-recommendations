package it.unibo.clar

import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Main extends App {
  /*
   * Loading Spark.
   */
  val sparkSession = SparkSession.builder
    .master("local[*]") // local[*] Run Spark locally with as many worker threads as logical cores on your machine
    .appName("CollaborativeLocationActivityRecommendations")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("WARN")
  Config.DEFAULT_PARALLELISM = sparkSession.sparkContext.defaultParallelism

  /*
   * Loading the dataset.
   */
  val path = "data/example.csv"
  //val path = "data/geolife_trajectories_complete.csv"
  val datasetCSV = sparkSession.read
    .option("header", value = true)
    .option("timestampFormat", TimestampFormatter.timestampPattern)
    .csv(path)
    .drop("label")

  /*
   * Loading and caching the RDD.
   */
  val datasetRDD = datasetCSV.rdd.map(row => (row(4).toString.toInt, new DatasetPoint(
    latitude = row(1).toString,
    longitude = row(2).toString,
    timestamp = row(0).toString
  )))

  val partitioner = new RangePartitioner(Config.DEFAULT_PARALLELISM, datasetRDD)
  val datasetRanged = datasetRDD.partitionBy(partitioner)

  /*
   * Algorithm implementation.
   */
  val pointsByUser = datasetRanged.groupByKey().persist(StorageLevel.MEMORY_AND_DISK)

  pointsByUser.foreach(pair => {
    val userId = pair._1
    val trajectory = sparkSession.sparkContext.parallelize(pair._2.toSeq)

    //val stayPoints = compute(trajectory).count()
    val stayPoints = computeStayPoints(pair._2.toSeq).size

    println(s"USER: ${userId} STAY POINTS COMPUTED: ${stayPoints}")
  })
}
