package it.unibo.clar

class StayRegion (val latitude: Double, val longitude: Double) extends Serializable {
  def this(stayPoints: Iterable[StayPoint]) = this(
    latitude = stayPoints.map(_.latitude).sum/stayPoints.size,
    longitude = stayPoints.map(_.longitude).sum/stayPoints.size,
  )
}
