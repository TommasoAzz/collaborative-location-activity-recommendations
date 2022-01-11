package it.unibo

import com.fasterxml.jackson.module.scala.deser.overrides.MutableList
import com.github.nscala_time.time.Imports._
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.joda.time.Seconds

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

  def compute(points: RDD[Point]): RDD[Point] ={


    val trajectory = points
      .zipWithIndex()
      .map { case (k, v) => (v, k) }
    val partitionSize = sqrt(trajectory.count()).toInt
    val trajectoryRanged = trajectory.partitionBy(new RangePartitioner(partitionSize, trajectory))
    val stayPoints = trajectoryRanged.mapPartitions(partition => {
      computeStayPoint(partition.map(_._2).toSeq).iterator
    })
    if(points.count()-stayPoints.count()>=Config.LOOP_THRESHOLD){
      compute(stayPoints)
    }else {
      stayPoints
    }
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



        val distance = (ith_element,jth_element)match {
          case (dp:DatasetPoint,dp2: DatasetPoint)=>{Haversine.haversine(dp.latitude, dp.longitude,
            dp2.latitude, dp2.longitude)}
          case (sp:StayPoint,dp: DatasetPoint)=>{Haversine.haversine(sp.firstPoint.latitude, sp.firstPoint.longitude,
            dp.latitude, dp.longitude)}
          case (dp:DatasetPoint,sp: StayPoint)=>{Haversine.haversine(dp.latitude, dp.longitude,
            sp.lastPoint.latitude, sp.lastPoint.longitude)}
          case (sp:StayPoint,sp2: StayPoint)=>{Haversine.haversine(sp.firstPoint.latitude, sp.firstPoint.longitude,
            sp2.lastPoint.latitude, sp.lastPoint.longitude)}
        }

        inside = distance <= Config.DISTANCE_THRESHOLD
        //if (inside) {
          //currentPoints += jth_element
        //}

        j += 1
      }
      //TIME CHECK
      val currentPoints = partition.slice(i, j)

      if (Seconds.secondsBetween(ith_element.getTime(), currentPoints.last.getTime()).getSeconds >= Config.TIME_THRESHOLD) {
        points += StayPoint(
          latitude=currentPoints.map(_.latitude).sum/currentPoints.size,
          longitude=currentPoints.map(_.longitude).sum/currentPoints.size,
          firstPoint=ith_element,
          lastPoint=currentPoints.last,
          contributingPoints=currentPoints.size,
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
