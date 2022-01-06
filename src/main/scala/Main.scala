package it.unibo.clar
import org.apache.spark.sql.SparkSession


object Main extends App {
  /*
   * Loading Spark.
   */
  val spark = SparkSession.builder
    .master("local[*]") // local[*] Run Spark locally with as many worker threads as logical cores on your machine
    .appName("CollaborativeLocationActivityRecommendations")
    .getOrCreate()

  /*
   * Loading the dataset.
   */
  val path = "data/example.csv"
  val datasetCSV = spark.read
    .option("header", value = true)
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .csv(path)
    .drop("label")

  /*
   * Loading and caching the RDD.
   */
  val datasetRDD = datasetCSV.rdd.map(row => (row(4).toString.toInt, pointFromRDDRow(
    row(1).toString,
    row(2).toString,
    row(3).toString,
    row(0).toString
  ))).cache()

  /*
   * Algorithm implementation.
   */
  val pointsByUser = datasetRDD.groupByKey()

  pointsByUser.foreach(pair => {
    val userId = pair._1
    val trajectory = pair._2

    println(s"USER: $userId")
    println(s"TRAJECTORY LENGTH: ${trajectory.size}")
  })
}
