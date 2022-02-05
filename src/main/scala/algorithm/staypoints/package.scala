package it.unibo.clar
package algorithm

import model.{DatasetPoint, StayPoint}
import config.{AlgorithmConfig, SparkProjectConfig}
import algorithm.staypoints.Executions.Execution

import it.unibo.clar.Main.sparkContext
import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.joda.time.Seconds

import scala.collection.mutable.ListBuffer

package object staypoints {
  def computeStayPoints(sparkContext: SparkContext)(datasetPoints: RDD[(Int, Iterable[DatasetPoint])])(execution: Execution): RDD[StayPoint] = {
    execution match {
      case Executions.Sequential => sparkContext
        .parallelize(datasetPoints
          .map(pair => _computeStayPoints(pair._2.toSeq))
          .reduce(_ ++ _)
        )
      case Executions.Parallel => datasetPoints
        .collectAsMap()
        .map(pair => sparkContext.parallelize(pair._2.toSeq))
        .map(_.map(t => (t.timestamp.toInstant.getMillis, t)))
        .map(trajectory => trajectory.partitionBy(new RangePartitioner(SparkProjectConfig.DEFAULT_PARALLELISM, trajectory)))
        .map(_.mapPartitions(partition => _computeStayPoints(partition.toSeq.map(_._2)).iterator))
        .reduce(_ ++ _)
    }
  }

  private def _computeStayPoints(partition: Seq[DatasetPoint]): Seq[StayPoint] = {
    val points = new ListBuffer[StayPoint] // [SP, SP, P, SP, P, P, P, SP]
    var i = 0
    while (i < partition.size) {
      val ith_element = partition(i)

      var j = i + 1
      var inside = true
      while (j < partition.size && inside) {
        val jth_element = partition(j)

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
