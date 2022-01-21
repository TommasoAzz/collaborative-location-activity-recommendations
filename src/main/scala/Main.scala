package it.unibo.clar

import org.apache.spark.RangePartitioner
import org.apache.spark.storage.StorageLevel

object Main extends App {
  /*
   * Checking arguments.
   */
  if(args.length != 3) {
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

  val allStayPointsSeq = pointsByUser.map(pair => {
    val userId = pair._1

    val stayPoints = computeStayPoints(pair._2.toSeq)

    println(s"USER: $userId STAY POINTS COMPUTED: ${stayPoints.size}")

    stayPoints
  }).reduce((sp1, sp2) => sp1 ++ sp2)
  val allStayPoints = sparkContext.parallelize(allStayPointsSeq)

  val gridCells = allStayPoints.map(sp => (computeGridPosition(sp.longitude, sp.latitude), sp))
    .groupByKey()

  gridCells.foreach(pair => println(s"CELL INDEX: ${pair._1} HAS ${pair._2.size} POINTS"))
  println(s"Total grid cells computed ${gridCells.count()}")

  sparkSession.stop()
}
