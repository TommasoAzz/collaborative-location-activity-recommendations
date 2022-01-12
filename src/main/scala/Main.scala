package it.unibo.clar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Main extends App {
  /*
   * Loading Spark.
   */
  val sparkSession = SparkSession.builder
    .master("local[*]") // local[*] Run Spark locally with as many worker threads as logical cores on your machine
    .appName("CollaborativeLocationActivityRecommendations")
    .getOrCreate()

  /*
   * Loading the dataset.
   */
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

  pointsByUser.foreach(pair => {
    val userId = pair._1
    val trajectory: RDD[Point] = sparkSession.sparkContext.parallelize(pair._2.toSeq)

    // val stayPoints = compute(trajectory).count()
    val stayPoints = computeStayPoints(trajectory.collect()).count(_.isInstanceOf[StayPoint])

    println(s"USER: ${userId} STAY POINTS COMPUTED: ${stayPoints}")

    //println(indexKey.map { case (k, v) => s"(${k}, ${v})" }.collect().mkString("Array(", ", ", ")"))
  })
}
