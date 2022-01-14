package it.unibo.clar

import com.github.nscala_time.time.Imports.DateTime

case class DatasetPoint(
                         override val latitude: Double,
                         override val longitude: Double,
                         override val timestamp: DateTime
                       )
  extends Point(latitude, longitude, timestamp) {
  def this(latitude: String, longitude: String, timestamp: String) = this(
    latitude.toDouble,
    longitude.toDouble,
    TimestampFormatter(timestamp)
  )
}