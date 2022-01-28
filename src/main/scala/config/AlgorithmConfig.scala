package it.unibo.clar
package config

object AlgorithmConfig {
  val DISTANCE_THRESHOLD = 200 // meters
  val TIME_THRESHOLD = 1200 // seconds

  val STAY_REGION_SIDE_LENGTH = 1000.0 // length of the side of the stay region square
  val GRID_CELL_SIDE_LENGTH: Double = STAY_REGION_SIDE_LENGTH / 3

  val WORLD_TOP_LEFT_LONGITUDE: Double = 0.0
  val WORLD_TOP_LEFT_LATITUDE: Double = 0.0

  val WORLD_BOTTOM_RIGHT_LONGITUDE: Double = 360.0
  val WORLD_BOTTOM_RIGHT_LATITUDE: Double = 180.0

  val STEP: Double = GRID_CELL_SIDE_LENGTH / 111111 // less accurate but more simpler

  val NUM_CELLS_LATITUDE: Int = math.floor((WORLD_BOTTOM_RIGHT_LATITUDE - WORLD_TOP_LEFT_LATITUDE) / STEP).toInt
  val NUM_CELLS_LONGITUDE: Int = math.floor((WORLD_BOTTOM_RIGHT_LONGITUDE - WORLD_TOP_LEFT_LONGITUDE) / STEP).toInt

  val MIN_NUM_STAY_POINTS_PER_REGION: Int = 3
}
