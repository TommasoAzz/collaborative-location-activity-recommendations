package it.unibo.clar

import org.apache.spark.RangePartitioner
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ListBuffer

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
  val sparkSession = Config.sparkSession(args(0))
  val sparkContext = sparkSession.sparkContext
  //Config.loadHadoop()

  /*
   * Loading the dataset.
   */
  val datasetCSV = sparkSession.read
    .option("header", value = true)
    .option("timestampFormat", TimestampFormatter.timestampPattern)
    .csv(datasetPath)
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

  //  val allStayPoints = pointsByUser.collectAsMap().map(pair => {
  //    val userId = pair._1
  //
  //    val trajectory = sparkContext.parallelize(pair._2.toSeq)
  //    val stayPoints = compute(trajectory)
  //
  //    //stayPoints.saveAsTextFile(s"$outputFolder/$userId/")
  //    println(s"USER: $userId STAY POINTS COMPUTED: ${stayPoints.count()}")
  //
  //    stayPoints
  //  }).reduce((sp1, sp2) => sp1 ++ sp2)

  val allStayPointsSeq = pointsByUser
    .map(pair => computeStayPoints(pair._2.toSeq))
    .reduce((sp1, sp2) => sp1 ++ sp2)
  println("Number of stay points: " + allStayPointsSeq.size)
  val allStayPoints = sparkContext.parallelize(allStayPointsSeq)


  val gridCells = allStayPoints
    .map(sp => (computeGridPosition(sp.longitude, sp.latitude), sp))
    .groupByKey()
    .map(gridCell => new GridCell(gridCell._1, gridCell._2))


  val listRatios = new ListBuffer[Double]

  val gridCellPartitioner = new GridCellPartitioner(Config.DEFAULT_PARALLELISM)
  val stayRegions = gridCells.map(gc => (gc.position, gc)).partitionBy(gridCellPartitioner)
    .mapPartitions(pairs => {
      val gridCells = pairs.map(_._2)
      val sortedCells = gridCells.toSeq
        .sortBy(pair => pair.stayPoints.size)(ord = Ordering.Int.reverse) //pair.gridCell.stayPoints

      if (sortedCells.isEmpty) {
        Iterator()
      }

      val stayRegionsAndRatios = for {
        i <- sortedCells.indices
        if !sortedCells(i).assigned //if not already assigned to a stay region
      } yield computeStayRegion(i, sortedCells)
      val stayRegionsAndRatiosWithoutOutliers = stayRegionsAndRatios.filter(_._3 >= Config.MIN_NUM_STAY_POINTS_PER_REGION)
      listRatios ++= stayRegionsAndRatiosWithoutOutliers.map(_._2)
      stayRegionsAndRatiosWithoutOutliers.map(_._1).iterator
    })

  /*    val stayRegions = gridCells.mapPartitions(gridCells => {
        val sortedCells = gridCells.toSeq
          .sortBy(pair => pair.stayPoints.size)(ord = Ordering.Int.reverse) //pair.gridCell.stayPoints

        val stayRegionsAndRatios = for {
          i <- sortedCells.indices
          if !sortedCells(i).assigned //if not already assigned to a stay region
        } yield computeStayRegion(i, sortedCells)
        listRatios ++= stayRegionsAndRatios.map(_._2)
        stayRegionsAndRatios.map(_._1).iterator
      })*/

  println("Number of stay regions: " + stayRegions.count())
  println("Mean neighbour ratio: " + listRatios.sum / listRatios.size)

  sparkSession.createDataFrame(stayRegions.map(sr => (sr.longitude, sr.latitude)))
    .toDF("longitude", "latitude")
    .coalesce(1)
    .write
    .option("header", value = true)
    .mode("overwrite")
    .csv(outputFolder + "/stayRegions")

  sparkSession.createDataFrame(allStayPoints.map(sp => (sp.longitude, sp.latitude, sp.timeOfArrival.toString(TimestampFormatter.formatter), sp.timeOfLeave.toString(TimestampFormatter.formatter))))
    .toDF("longitude", "latitude", "timeOfArrival", "timeOfLeave")
    .coalesce(1)
    .write
    .option("header", value = true)
    .mode("overwrite")
    .csv(outputFolder + "/stayPoints")


  sparkSession.stop()
}
