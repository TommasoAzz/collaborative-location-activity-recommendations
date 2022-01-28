package it.unibo.clar
package model

import com.github.nscala_time.time.Imports.DateTime
import utils.TimestampFormatter

case class StayPoint(
                      override val latitude: Double,
                      override val longitude: Double,
                      timeOfArrival: DateTime,
                      timeOfLeave: DateTime
                    )
  extends Point(latitude, longitude, timeOfLeave) {
  def toCSVTuple: (Double, Double, String, String) = (
    longitude,
    latitude,
    timeOfArrival.toString(TimestampFormatter.formatter),
    timeOfLeave.toString(TimestampFormatter.formatter)
  )
}
