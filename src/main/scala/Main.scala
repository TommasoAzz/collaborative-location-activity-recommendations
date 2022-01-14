package it.unibo.clar

import com.github.nscala_time.time.Imports.DateTime
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


object Main extends App {
  /*
   * Loading Spark.
   */
  val sparkSession = SparkSession.builder
    .master("local[*]") // local[*] Run Spark locally with as many worker threads as logical cores on your machine
    .appName("CollaborativeLocationActivityRecommendations")
    .config("spark.executor.processTreeMetrics.enabled", value = false)
    .getOrCreate()
    Config.DEFAULT_PARTITIONS_NUMBER = sparkSession.sparkContext.defaultParallelism
  /*
   * Loading the dataset.
   */
  // val path = "data/geolife_trajectories_complete.csv"
  val path = "data/example.csv"
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
  ))).cache()

  /*
   * Algorithm implementation.
   */
  val pointsByUser = datasetRDD.groupByKey()

  val results = new ListBuffer[String]

  time(pointsByUser.foreach(pair => {
    val userId = pair._1
    val trajectory = sparkSession.sparkContext.parallelize(pair._2.toSeq)

    /**
     * Verificare il parametro numSlices di parallelize.
     */
    // val zippedTraj = trajectory.map(t => (t.timestamp.toInstant.getMillis, t))
    // val stayPoints = compute(zippedTraj).count()
    val stayPoints = computeStayPoints(pair._2.toSeq).size

    results += (s"USER: ${userId} STAY POINTS COMPUTED: ${stayPoints}")
  }))

  results.foreach(println)
}
