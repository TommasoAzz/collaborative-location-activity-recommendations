package it.unibo.clar

import com.github.nscala_time.time.Imports
import com.github.nscala_time.time.Imports.DateTime

case class StayPoint(
                               override val latitude: Double,
                               override val longitude: Double,
                               firstPoint: Point,
                               lastPoint: Point,
                               contributingPoints: Int
                             )
  extends Point(latitude, longitude,firstPoint.getTime()) {


  override def getTime(): DateTime = firstPoint.getTime()

  def timeOfArrival = firstPoint.getTime()

  def timeOfLeave = lastPoint.getTime()

}
