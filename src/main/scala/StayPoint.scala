package it.unibo.clar

import com.github.nscala_time.time.Imports.DateTime

case class StayPoint(
                      override val latitude: Double,
                      override val longitude: Double,
                      firstPoint: DatasetPoint,
                      contributingPoints: Int,
                      timeOfArrival: DateTime,
                      timeOfLeave: DateTime
                    )
  extends Point(latitude, longitude, timeOfArrival)
