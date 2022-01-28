package it.unibo.clar
package model

case class StayRegion(latitude: Double, longitude: Double) extends Serializable {
  def this(stayPoints: Iterable[StayPoint]) = this(
    latitude = stayPoints.map(_.latitude).sum / stayPoints.size,
    longitude = stayPoints.map(_.longitude).sum / stayPoints.size,
  )
}
