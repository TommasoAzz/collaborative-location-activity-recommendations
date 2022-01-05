package it.unibo.clar
import org.apache.spark.sql.SparkSession


object Main extends App{

  val path = "data/example.csv"

  val spark = SparkSession.builder()
    .master("local[*]") // local[*] Run Spark locally with as many worker threads as logical cores on your machine
  .appName("CollaborativeLocationActivityRecommendations")
  .getOrCreate()
  val df = spark.read.csv(path)
  df.show()


}
