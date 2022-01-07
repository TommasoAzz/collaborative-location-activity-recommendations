package it.unibo.clar

import com.github.nscala_time.time.Imports.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


object TimestampFormatter {
  val timestampPattern = "yyyy-MM-dd HH:mm:ss"

  val formatter: DateTimeFormatter = DateTimeFormat.forPattern(timestampPattern).withZoneUTC()

  def apply(timestamp: String): DateTime = formatter.parseDateTime(timestamp)
}
