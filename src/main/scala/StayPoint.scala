package it.unibo.clar

import com.github.nscala_time.time.Imports.DateTime

case class StayPoint(
                      override val latitude: Double,
                      override val longitude: Double,
                      timeOfArrival: DateTime,
                      timeOfLeave: DateTime
                    )
  extends Point(latitude, longitude, timeOfLeave) {
}
