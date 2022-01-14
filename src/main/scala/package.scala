package it.unibo

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.joda.time.Seconds

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.math.sqrt

package object clar {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  // @tailrec
  def compute(points: RDD[DatasetPoint]): RDD[StayPoint] = {
    // val trajectory = points.zipWithIndex().map { case (point, index) => (index, point) }
    val trajectory = points.map(t => (t.timestamp.toInstant.getMillis, t))
    val partitionSize = sqrt(trajectory.count()).toInt
    val partitioner = new RangePartitioner(partitionSize, trajectory)
    val trajectoryRanged = trajectory.partitionBy(partitioner)

    val stayPoints = trajectoryRanged.mapPartitions(partition => {
      val saved = computeStayPoints(partition.map(_._2).toSeq).iterator
      println(saved.size)
      saved
    })
//    if(points.count() - stayPoints.count() >= Config.LOOP_THRESHOLD) {
//      compute(stayPoints)
//    } else {
//      stayPoints.filter(p => p.isInstanceOf[StayPoint])
//    }
    stayPoints
  }

  def computeStayPoints(partition: Seq[DatasetPoint]): Seq[StayPoint] = {
    val points = new ListBuffer[StayPoint] // [SP, SP, P, SP, P, P, P, SP]

    var i = 0
    while (i < partition.size) {
      val ith_element = partition(i)

      var j = i + 1
      var inside = true
      while (j < partition.size && inside) {
        val jth_element = partition(j)
        // DISTANCE CHECK
        /*val latitudesLongitudes = (ith_element, jth_element) match {
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
        )*/
        val distance = Haversine.haversine(
          lat1 = ith_element.latitude,
          lon1 = ith_element.longitude,
          lat2 = jth_element.latitude,
          lon2 = jth_element.longitude
        )
        inside = distance <= Config.DISTANCE_THRESHOLD
        j += 1
      }
      // TIME CHECK
      val currentPoints = partition.slice(i, j)

      /*val times = (ith_element, currentPoints.last) match {
        case (dp1: DatasetPoint, dp2: DatasetPoint) => (dp1.timestamp, dp2.timestamp)
        case (sp: StayPoint, dp: DatasetPoint) => (sp.firstPoint.timestamp, dp.timestamp)
        case (dp: DatasetPoint, sp: StayPoint) => (dp.timestamp, sp.lastPoint.timestamp)
        case (sp1: StayPoint, sp2: StayPoint) => (sp1.firstPoint.timestamp, sp2.lastPoint.timestamp)
      }
      val timeDelta = Seconds.secondsBetween(times._1, times._2).getSeconds*/
      val timeDelta = Seconds.secondsBetween(ith_element.timestamp, currentPoints.last.timestamp).getSeconds

      if (timeDelta >= Config.TIME_THRESHOLD) {
        // val totalPoints = currentPoints.map(_.cardinality).sum
        val totalPoints = j - i
        points += StayPoint(
          latitude = currentPoints.map(_.latitude).sum / totalPoints,
          longitude = currentPoints.map(_.longitude).sum / totalPoints,
//          firstPoint=ith_element,
//          lastPoint=currentPoints.last,
//          contributingPoints=totalPoints,
          timeOfArrival = ith_element.timestamp,
          timeOfLeave = currentPoints.last.timestamp
        )
      }
//      else {
//        points ++= currentPoints
//      }

      i = j
    }

    points.toSeq
  }
}
