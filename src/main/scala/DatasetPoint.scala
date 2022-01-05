package it.unibo.clar

import com.github.nscala_time.time.Imports.DateTime

case class DatasetPoint(
                         override val latitude: Double,
                         override val longitude: Double,
                         override val altitude: Double,
                         override val timestamp: DateTime
                       )
  extends Point(latitude, longitude, altitude, timestamp) {
  def this(lat: String, lon: String, alt: String, time: String) = this(
    lat.toDouble,
    lon.toDouble,
    alt.toDouble,
    TimestampFormatter(time)
  )
}
