package it.unibo.clar

import com.github.nscala_time.time.Imports.DateTime

abstract class Point(
                      val latitude: Double,
                      val longitude: Double,
                      val altitude: Double,
                      val timestamp: DateTime
                    )


