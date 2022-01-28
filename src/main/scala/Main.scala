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
  if (args.length != 3) {
    println("Missing arguments")
    throw new MissingConfigurationException
  }
  val master = args(0)
  val datasetPath = args(1)
  val outputFolder = args(2)

  /*
   * Loading Spark and Hadoop.
   */
  val sparkSession = SparkProjectConfig.sparkSession(args(0))
  val sparkContext = sparkSession.sparkContext

  /*
   * Loading the dataset.
   */
  val datasetCSV = sparkSession.read
    .option("header", value = true)
    .option("timestampFormat", TimestampFormatter.timestampPattern)
    .csv(datasetPath)
    .drop("label")

  /*
   * Loading, partitioning and caching the RDD.
   */
  val datasetRDD = datasetCSV.rdd.map(row => (row(4).toString.toInt, new DatasetPoint(
    latitude = row(1).toString,
    longitude = row(2).toString,
    timestamp = row(0).toString
  )))

  val partitioner = new RangePartitioner(SparkProjectConfig.DEFAULT_PARALLELISM, datasetRDD)
  val datasetRanged = datasetRDD.partitionBy(partitioner)

  /*
   * Algorithm implementation.
   */

  // (1) Split dataset into trajectories for each user
  val pointsByUser = datasetRanged.groupByKey().persist(StorageLevel.MEMORY_AND_DISK)

  // (2) Compute the stay points
  // val allStayPoints = algorithm.staypoints.computeStayPoints(sparkContext)(pointsByUser)(Executions.Sequential)
  val allStayPoints = algorithm.staypoints.computeStayPoints(sparkContext)(pointsByUser)(Executions.Parallel)

  println("Number of stay points: " + allStayPoints.count())

  // (3) Associate the computed stay points to a specific grid cell. The whole grid refers to the entire world.
  val gridCells = allStayPoints
    .map(sp => (algorithm.gridcells.computeGridPosition(sp.longitude, sp.latitude), sp))
    .groupByKey()
    .map(gridCell => new GridCell(gridCell._1, gridCell._2))

  // (4) Compute stay regions from the grid cells output of (3)
  val stayRegions = algorithm.stayregions.computeStayRegions(Partitionings.GridCell)(gridCells)
  // val stayRegions = algorithm.stayregions.computeStayRegions(Partitionings.Hash)(gridCells)

  println("Number of stay regions: " + stayRegions.count())

  // (5) Saving the output into CSV files
  sparkSession.createDataFrame(stayRegions.map(_.toCSVTuple))
    .toDF("longitude", "latitude")
    .coalesce(1)
    .write
    .option("header", value = true)
    .mode("overwrite")
    .csv(outputFolder + "/stayRegions")

  sparkSession.createDataFrame(allStayPoints.map(_.toCSVTuple))
    .toDF("longitude", "latitude", "timeOfArrival", "timeOfLeave")
    .coalesce(1)
    .write
    .option("header", value = true)
    .mode("overwrite")
    .csv(outputFolder + "/stayPoints")

  sparkSession.stop()
}
