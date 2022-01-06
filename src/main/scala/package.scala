package it.unibo

package object clar {
  def pointFromRDDRow(lat: String, lon: String, alt: String, time: String): DatasetPoint = DatasetPoint(
    lat.toDouble,
    lon.toDouble,
    alt.toDouble,
    TimestampFormatter(time)
  )
}
