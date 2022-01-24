package it.unibo.clar

class GridCell (val position: (Int, Int),
                val stayPoints: Iterable[StayPoint],
                var assigned: Boolean = false) extends Serializable
