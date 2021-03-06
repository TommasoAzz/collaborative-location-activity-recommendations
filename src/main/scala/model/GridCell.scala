package it.unibo.clar
package model

class GridCell(val position: (Int, Int), // Longitude(X), Latitude(Y)
               val stayPoints: Iterable[StayPoint],
               var assigned: Boolean = false) extends Serializable
