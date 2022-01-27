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

    points
  }

  def computeGridPosition(longitude: Double, latitude: Double): (Int, Int) = {

    val shiftedLong = longitude+180
    val shiftedLat = math.abs(latitude-90)

    val cellX = math.floor((shiftedLong) / Config.STEP).toInt
    val cellY = math.floor((shiftedLat) / Config.STEP).toInt
    (cellX, cellY)
  }

  def computeStayRegion(index: Int, gridCells: Seq[GridCell]): (StayRegion, Double, Int) = {
    val gridCell = gridCells(index)
    val posLat = gridCell.position._2
    val posLong = gridCell.position._1
    val neighbours = gridCells.filter(gridCell => {
      val nLat = gridCell.position._2
      val nLong = gridCell.position._1
      val conditionLong = ((posLong-1) % Config.NUM_CELLS_LONGITUDE) == nLong ||
        (posLong % Config.NUM_CELLS_LONGITUDE) == nLong ||
        ((posLong+1) % Config.NUM_CELLS_LONGITUDE) == nLong
      val conditionLat = ((posLat-1) % Config.NUM_CELLS_LATITUDE) == nLat ||
        (posLat % Config.NUM_CELLS_LATITUDE) == nLat ||
        ((posLat+1) % Config.NUM_CELLS_LATITUDE) == nLat

      gridCell.assigned = conditionLong && conditionLat
      conditionLong && conditionLat
    })

    val neighbourRatio = neighbours.size / 9.0

    val neighbouringStayPoints = neighbours.flatMap(_.stayPoints)

    (new StayRegion(stayPoints = neighbouringStayPoints), neighbourRatio, neighbouringStayPoints.size)
  }
}
