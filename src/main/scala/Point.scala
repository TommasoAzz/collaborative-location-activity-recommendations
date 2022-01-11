package it.unibo.clar

import com.github.nscala_time.time.Imports.DateTime

abstract class Point(
                      val latitude: Double,
                      val longitude: Double,
                      val timestamp: DateTime
                    ) extends Serializable{
  abstract def getTime():DateTime
}


