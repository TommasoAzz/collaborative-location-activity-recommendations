package it.unibo.clar
package algorithm

import model.{DatasetPoint, StayPoint}
import config.{AlgorithmConfig, SparkProjectConfig}
import algorithm.staypoints.Executions.Execution

import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.joda.time.Seconds

import scala.collection.mutable.ListBuffer

package object staypoints {
  def computeStayPoints(sparkContext: SparkContext)(csvDataset: DataFrame)(execution: Execution): RDD[StayPoint] = {
    execution match {
      case Executions.Sequential =>
        val datasetRdd = csvDataset.rdd.map(_fromDataFrameRowToRDDPair)
        val datasetRanged = datasetRdd.partitionBy(new RangePartitioner(SparkProjectConfig.DEFAULT_PARALLELISM, datasetRdd))
        val pointsByUser = datasetRanged.groupByKey().persist(StorageLevel.MEMORY_AND_DISK)
        sparkContext.parallelize(
          pointsByUser.map(
            pair => _computeStayPoints(pair._2.toSeq)
          ).reduce(_ ++ _)
        )
      case Executions.Parallel =>
        val cardinality = csvDataset.agg(("user", "max")).collect()(0).getString(0).toInt + 1
        val rdds = for {
          i <- 0 until cardinality
        } yield csvDataset.where(s"user == $i").rdd.map(_fromDataFrameRowToRDDPair).persist(StorageLevel.MEMORY_AND_DISK)
        val rangedRdds = rdds.map(rdd => rdd.partitionBy(new RangePartitioner(SparkProjectConfig.DEFAULT_PARALLELISM, rdd)))
        rangedRdds.map(
          _.mapPartitions(partition => _computeStayPoints(partition.toSeq.map(_._2)).iterator)
        ).reduce(_ ++ _)
    }
  }

  private def _fromDataFrameRowToRDDPair(row: Row): (Int, DatasetPoint) = (
    row(4).toString.toInt, new DatasetPoint(
    latitude = row(1).toString,
    longitude = row(2).toString,
    timestamp = row(0).toString
  )
  )

  private def _computeStayPoints(partition: Seq[DatasetPoint]): Seq[StayPoint] = {
    val points = new ListBuffer[StayPoint]
    var i = 0
    while (i < partition.size) {
      val ith_element = partition(i)

      var j = i + 1
      var inside = true
      while (j < partition.size && inside) {
        val jth_element = partition(j)
        // DISTANCE CHECK
        val distance = Haversine(
          lat1 = ith_element.latitude,
          lon1 = ith_element.longitude,
          lat2 = jth_element.latitude,
          lon2 = jth_element.longitude
        )
        inside = distance <= AlgorithmConfig.DISTANCE_THRESHOLD
        j += 1
      }
      // TIME CHECK
      val currentPoints = partition.slice(i, j)

      val timeDelta = Seconds.secondsBetween(ith_element.timestamp, currentPoints.last.timestamp).getSeconds

      if (timeDelta >= AlgorithmConfig.TIME_THRESHOLD) {
        val totalPoints = j - i
        points += model.StayPoint(
          latitude = currentPoints.map(_.latitude).sum / totalPoints,
          longitude = currentPoints.map(_.longitude).sum / totalPoints,
          timeOfArrival = ith_element.timestamp,
          timeOfLeave = currentPoints.last.timestamp
        )
      }

      i = j
    }

    points
  }
}
