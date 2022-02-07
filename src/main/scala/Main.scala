package it.unibo.clar

import algorithm.stayregions.Partitionings
import config.SparkProjectConfig
import exception.MissingConfigurationException
import model.{DatasetPoint, GridCell}
import utils.TimestampFormatter
import algorithm.staypoints.Executions

import org.apache.spark.RangePartitioner
import org.apache.spark.storage.StorageLevel

object Main extends App {
  /*
   * Checking arguments.
   */
  if (args.length != 6) {
    println("Missing arguments")
    throw new MissingConfigurationException
  }
  val master = args(0)
  val datasetPath = args(1)
  val outputFolder = args(2)
  val stayPointExecution = if(args(3) == "sp=parallel") Executions.Parallel else Executions.Sequential
  val stayRegionPartitioning = if(args(4) == "sr=hash") Partitionings.Hash else Partitionings.GridCell
  val parallelism = args(5)

  println(s"Master: $master")
  println(s"Dataset path: $datasetPath")
  println(s"Output folder: $outputFolder")
  println(s"Stay Point execution: $stayPointExecution")
  println(s"Stay Region partitioning: $stayRegionPartitioning")
  println(s"Initialized Spark Context with parallelism: ${SparkProjectConfig.DEFAULT_PARALLELISM}")

  /*
   * Loading Spark and Hadoop.
   */
  val sparkSession = SparkProjectConfig.sparkSession(master, parallelism.toInt)
  val sparkContext = sparkSession.sparkContext

  /*
   * Loading the dataset.
   */


  val datasetCSV = sparkSession.read
    .option("header", value = true)
    .option("timestampFormat", TimestampFormatter.timestampPattern)
    .csv(datasetPath)
    .drop("label")

  println("Dataset read")

  /*
   * Loading, partitioning and caching the RDD.
   */
  val datasetRDD = datasetCSV.rdd.map(row => (row(4).toString.toInt, new DatasetPoint(
    latitude = row(1).toString,
    longitude = row(2).toString,
    timestamp = row(0).toString
  )))

  println("Datasetpoint rdd created")


  val partitioner = new RangePartitioner(SparkProjectConfig.DEFAULT_PARALLELISM, datasetRDD)
  val datasetRanged = datasetRDD.partitionBy(partitioner)

  /*
   * Algorithm implementation.
   */

  // (1) Split dataset into trajectories for each user
  val pointsByUser = datasetRanged.groupByKey().persist(StorageLevel.MEMORY_AND_DISK)
  println("Partitioned")
  // (2) Compute the stay points
  val computeStayPoints = algorithm.staypoints.computeStayPoints(sparkContext)(pointsByUser) _ // The underscore means "partial application".
  val allStayPoints = time("computeStayPoints", computeStayPoints(stayPointExecution))

  println("Number of stay points: " + time("--> action", allStayPoints.count()))
  allStayPoints.persist(StorageLevel.MEMORY_AND_DISK)

  // (3) Associate the computed stay points to a specific grid cell. The whole grid refers to the entire world.
  val gridCells = time("computeGridPosition", allStayPoints
    .map(sp => (algorithm.gridcells.computeGridPosition(sp.longitude, sp.latitude), sp))
    .groupByKey()
    .map(gridCell => new GridCell(gridCell._1, gridCell._2)))

  time("--> action", gridCells.collect())

  // (4) Compute stay regions from the grid cells output of (3)
  val computeStayRegions = algorithm.stayregions.computeStayRegions(gridCells) _ // The underscore means "partial application".
  val stayRegions = time("computeStayRegions", computeStayRegions(stayRegionPartitioning))

  println("Number of stay regions: " + time("--> action", stayRegions.count()))
  /*
  stayRegions.persist(StorageLevel.MEMORY_AND_DISK)

  // (5) Saving the output into CSV files
  sparkSession.createDataFrame(allStayPoints.map(_.toCSVTuple))
    .toDF("longitude", "latitude", "timeOfArrival", "timeOfLeave")
    .coalesce(1)
    .write
    .option("header", value = true)
    .mode("overwrite")
    .csv(outputFolder + "/stayPoints")

  sparkSession.createDataFrame(stayRegions.map(_.toCSVTuple))
    .toDF("longitude", "latitude")
    .coalesce(1)
    .write
    .option("header", value = true)
    .mode("overwrite")
    .csv(outputFolder + "/stayRegions")
*/
  sparkSession.stop()
}
