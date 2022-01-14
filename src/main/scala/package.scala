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

  def compute(points: RDD[DatasetPoint]/*, iteration_index: Int = 0*/): RDD[StayPoint] = {
    /*
     * Questo zipWithIndex() è overkill perché ogni volta deve scorrere l'RDD e rivoltarlo come un calzino.
     * Dopodiché c'è uno split in sqrt(|trajectory|) forse eccessivo.
     * Necessario mettere un cap alle iterazioni?
     */
    //val totalPartitions = pow(points.count(), 0.25).toInt
    val keyedPoints = points.map(t => (t.timestamp.toInstant.getMillis, t))
    val partitioner = new RangePartitioner(Config.DEFAULT_PARTITIONS_NUMBER, keyedPoints)
    val trajectoryRanged = keyedPoints.partitionBy(partitioner)

    val stayPoints = trajectoryRanged.mapPartitions(partition => {
      computeStayPoints(partition.toSeq.map(_._2)).iterator
    })
    stayPoints
//    if(points.count() - stayPoints.count() >= Config.CARDINALITY_DELTA /*&& iteration_index < Config.MAX_ITERATIONS*/) {
//      compute(stayPoints/*, iteration_index + 1*/)
//    } else {
//      stayPoints.filter(p => p._2.isInstanceOf[StayPoint])
//    }
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

      val timeDelta = Seconds.secondsBetween(ith_element.timestamp, ith_element.timestamp).getSeconds

      if (timeDelta >= Config.TIME_THRESHOLD) {
        val totalPoints = j - i
        points += StayPoint(
          latitude=currentPoints.map(_.latitude).sum / totalPoints,
          longitude=currentPoints.map(_.longitude).sum / totalPoints,
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
