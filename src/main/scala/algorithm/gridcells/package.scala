package it.unibo.clar
package algorithm

import config.AlgorithmConfig

import model.GridCell

package object gridcells {
  def computeGridPosition(longitude: Double, latitude: Double): (Int, Int) = {
    val shiftedLong = longitude+180
    val shiftedLat = math.abs(latitude-90)

    val cellX = math.floor(shiftedLong / AlgorithmConfig.STEP).toInt
    val cellY = math.floor(shiftedLat / AlgorithmConfig.STEP).toInt

    (cellX, cellY)
  }

  def sortGridCells(gridCells: Seq[GridCell]): Seq[GridCell] = {
    gridCells.sortBy(pair => pair.stayPoints.size)(ord = Ordering.Int.reverse)
  }
}
