package it.unibo.clar
package algorithm.staypoints

object Haversine {
  // Earth radius (WGS84)
  val R = 6378137d

  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)
    val lat1Rad = math.toRadians(lat1)
    val lat2Rad = math.toRadians(lat2)

    val a = math.pow(math.sin(dLat / 2), 2) + math.pow(math.sin(dLon / 2), 2) * math.cos(lat1Rad) * math.cos(lat2Rad)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    R * c
  }

  def apply(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = haversine(lat1, lon1, lat2, lon2)
}
