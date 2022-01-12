package it.unibo.clar

case class StayPoint(
                      override val latitude: Double,
                      override val longitude: Double,
                      firstPoint: Point,
                      lastPoint: Point,
                      contributingPoints: Int
                    )
  extends Point(latitude, longitude, firstPoint.timestamp) {
  override def cardinality: Int = contributingPoints
}
