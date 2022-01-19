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

  val pointsByUserRDDs = pointsByUser.map(pair => (pair._1, sparkContext.parallelize(pair._2.toSeq)))

  pointsByUserRDDs.foreach(pair => {
    val userId = pair._1
    val trajectory = pair._2
    val stayPoints = compute(trajectory)
    stayPoints.saveAsTextFile(s"$outputFolder/$userId/")
    println(s"USER: $userId STAY POINTS COMPUTED: ${stayPoints.count()}")
  })

  /*pointsByUser.foreach(pair => {
    val userId = pair._1
    val trajectory = sparkContext.parallelize(pair._2.toSeq)

    //val stayPoints = compute(trajectory).count()
    val stayPoints = computeStayPoints(pair._2.toSeq).size

    println(s"USER: $userId STAY POINTS COMPUTED: $stayPoints")
  })*/

  sparkSession.stop()
}
