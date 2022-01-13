package it.unibo

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.joda.time.Seconds

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.math.{pow, sqrt}

package object clar {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  @tailrec
  def compute(points: RDD[(Long,Point)], iteration_index: Int = 0): RDD[(Long,Point)] = {
    /*
     * Questo zipWithIndex() è overkill perché ogni volta deve scorrere l'RDD e rivoltarlo come un calzino.
     * Dopodiché c'è uno split in sqrt(|trajectory|) forse eccessivo.
     * Necessario mettere un cap alle iterazioni?
     */
    //val totalPartitions = pow(points.count(), 0.25).toInt
    val partitioner = new RangePartitioner(Config.DEFAULT_PARTITIONS_NUMBER, points)
    val trajectoryRanged = points.partitionBy(partitioner)

    val stayPoints = trajectoryRanged.mapPartitions(partition => {
      computeStayPoints(partition.toSeq).iterator
    })
    if(points.count() - stayPoints.count() >= Config.CARDINALITY_DELTA /*&& iteration_index < Config.MAX_ITERATIONS*/) {
      compute(stayPoints/*, iteration_index + 1*/)
    } else {
      stayPoints.filter(p => p._2.isInstanceOf[StayPoint])
    }
  }

  def computeStayPoints(partition: Seq[(Long,Point)]): Seq[(Long,Point)] = {
    val points = new ListBuffer[(Long,Point)] // [SP, SP, P, SP, P, P, P, SP]

    var i = 0
    while (i < partition.size) {
      val ith_element = partition(i)._2
      val newStayPointIndex = partition(i)._1

      var j = i + 1
      var inside = true
      while (j < partition.size && inside) {
        val jth_element = partition(j)._2
        // DISTANCE CHECK
        val latitudesLongitudes = (ith_element, jth_element) match {
          case (dp1: DatasetPoint, dp2: DatasetPoint) => (dp1.latitude, dp1.longitude, dp2.latitude, dp2.longitude)
          case (sp: StayPoint, dp: DatasetPoint) => (sp.firstPoint.latitude, sp.firstPoint.longitude, dp.latitude, dp.longitude)
          case (dp: DatasetPoint, sp: StayPoint) => (dp.latitude, dp.longitude, sp.lastPoint.latitude, sp.lastPoint.longitude)
          case (sp1: StayPoint, sp2: StayPoint) => (sp1.firstPoint.latitude, sp1.firstPoint.longitude, sp2.lastPoint.latitude, sp2.lastPoint.longitude)
        }
        val distance = Haversine.haversine(
          lat1 = latitudesLongitudes._1,
          lon1 = latitudesLongitudes._2,
          lat2 = latitudesLongitudes._3,
          lon2 = latitudesLongitudes._4
        )
        inside = distance <= Config.DISTANCE_THRESHOLD
        j += 1
      }
      // TIME CHECK
      val currentPoints = partition.slice(i, j)

      val times = (ith_element, currentPoints.last._2) match {
        case (dp1: DatasetPoint, dp2: DatasetPoint) => (dp1.timestamp, dp2.timestamp)
        case (sp: StayPoint, dp: DatasetPoint) => (sp.firstPoint.timestamp, dp.timestamp)
        case (dp: DatasetPoint, sp: StayPoint) => (dp.timestamp, sp.lastPoint.timestamp)
        case (sp1: StayPoint, sp2: StayPoint) => (sp1.firstPoint.timestamp, sp2.lastPoint.timestamp)
      }
      val timeDelta = Seconds.secondsBetween(times._1, times._2).getSeconds

      if (timeDelta >= Config.TIME_THRESHOLD) {
        val mappedPoints=currentPoints.map(_._2)
        val totalPoints = mappedPoints.map(_.cardinality).sum
        points +=((newStayPointIndex, StayPoint(
          latitude=mappedPoints.map(p => p.latitude * p.cardinality).sum / totalPoints,
          longitude=mappedPoints.map(p => p.longitude * p.cardinality).sum / totalPoints,
          firstPoint=ith_element,
          lastPoint=mappedPoints.last,
          contributingPoints=totalPoints,
        )))
      }
      else {
        points ++= currentPoints
      }

      i = j
    }

    points.toSeq
  }
}
