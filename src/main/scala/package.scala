package it.unibo

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
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

  def compute(points: RDD[DatasetPoint]): RDD[StayPoint] = {
    // val trajectory = points.zipWithIndex().map { case (point, index) => (index, point) }
    val trajectory = points.map(t => (t.timestamp.toInstant.getMillis, t))
    val partitioner = new RangePartitioner(Config.DEFAULT_PARALLELISM, trajectory)
    val trajectoryRanged = trajectory.partitionBy(partitioner)

    val stayPoints = trajectoryRanged.mapPartitions(partition => {
      computeStayPoints(partition.toSeq.map(_._2)).iterator
    })

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

        val distance = Haversine(
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

      val timeDelta = Seconds.secondsBetween(ith_element.timestamp, currentPoints.last.timestamp).getSeconds

      if (timeDelta >= Config.TIME_THRESHOLD) {
        val totalPoints = j - i
        points += StayPoint(
          latitude = currentPoints.map(_.latitude).sum / totalPoints,
          longitude = currentPoints.map(_.longitude).sum / totalPoints,
          timeOfArrival = ith_element.timestamp,
          timeOfLeave = currentPoints.last.timestamp
        )
      }

      i = j
    }

    points.toSeq
  }

  def computeGridPosition(longitude: Double, latitude: Double): (Int, Int) = {
    /*
     * World coordinates:
     * ((90.0, 180.0), (90.0, -180.0), (-90.0, -180.0), (-90.0, 180.0)) // NE, NW, SW, SE
    */

    // longitude = x
    // latitude = y
    // Given origin, to compute the grid cell in a matrix ranging from (0, 0) (bottom left)
    // to (90.0 / Config.GRID_CELL_SIDE_LENGTH, 180.0 / Config.GRID_CELL_SIDE_LENGTH)
    // (assuming Config.GRID_CELL_SIDE_LENGTH is in degrees, which is not) the formula
    // to comp
    val longitude_step = Config.GRID_CELL_SIDE_LENGTH / (111111 * math.cos(math.toRadians(latitude)))
    val latitude_step = Config.GRID_CELL_SIDE_LENGTH / 111111
    val cellX = math.floor((longitude - Config.WORLD_BOTTOM_LEFT_LONGITUDE) / longitude_step).toInt
    val cellY = math.floor((latitude - Config.WORLD_BOTTOM_LEFT_LATITUDE) / latitude_step).toInt

    (cellX, cellY)
  }
}
