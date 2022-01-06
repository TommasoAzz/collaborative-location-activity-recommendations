package it.unibo.clar
import org.apache.spark.sql.SparkSession


object Main extends App{

  val path = "data/example.csv"

  val spark = SparkSession.builder
    .master("local[1]") // local[*] Run Spark locally with as many worker threads as logical cores on your machine
    .appName("CollaborativeLocationActivityRecommendations")
    .getOrCreate()

  val datasetCSV = spark.read
    .option("header", value = true)
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .csv(path)
    .drop("label")




  val datasetRDD = datasetCSV.rdd


  datasetRDD.map(row =>(row(4), new DatasetPoint(
    row(1).toString,
    row(2).toString,
    row(3).toString,
    row(0). toString
  ))).foreach(println)



}
