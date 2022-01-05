package it.unibo.clar

import com.github.nscala_time.time.Imports.DateTime

case class StayPoint(
                      override val latitude: Double,
                      override val longitude: Double,
                      override val altitude: Double,
                      override val timestamp: DateTime,
                      firstPoint: DatasetPoint,
                      contributingPoints: Int
                    )
  extends Point(latitude, longitude, altitude, timestamp)
