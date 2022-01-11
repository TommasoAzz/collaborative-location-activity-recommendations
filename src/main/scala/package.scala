package it.unibo

import com.fasterxml.jackson.module.scala.deser.overrides.MutableList
import com.github.nscala_time.time.Imports._
import org.joda.time.Seconds

import scala.collection.mutable.ListBuffer

package object clar {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  def computeStayPoint(partition: Seq[Point]): Seq[Point] = {
    val points = new ListBuffer[Point]// [SP, SP, P, SP, P, P, P, SP]

    var i = 0
    while (i < partition.size) {
      val ith_element = partition(i)
      //val currentPoints = new ListBuffer[Point]
      //currentPoints += ith_element

      var j = i + 1
      var inside = true
      while (j < partition.size && inside) {
        val jth_element = partition(j)

        val distance = Haversine.haversine(ith_element.latitude, ith_element.longitude,
          jth_element.latitude, jth_element.longitude)
        inside = distance <= Config.DISTANCE_THRESHOLD
        //if (inside) {
          //currentPoints += jth_element
        //}

        j += 1
      }
      //TIME CHECK
      val currentPoints = partition.slice(i, j)

      if (Seconds.secondsBetween(ith_element.timestamp, currentPoints.last.timestamp).getSeconds >= Config.TIME_THRESHOLD) {
        points += StayPoint(
          latitude=currentPoints.map(_.latitude).sum/currentPoints.size,
          longitude=currentPoints.map(_.longitude).sum/currentPoints.size,
          firstPoint=ith_element.asInstanceOf[DatasetPoint],
          contributingPoints=currentPoints.size,
          timeOfArrival = ith_element.timestamp,
          timeOfLeave = currentPoints.last.timestamp
        )
      }
      else {
        points ++= currentPoints
      }

      i = j
    }

    points.toSeq
  }
}
