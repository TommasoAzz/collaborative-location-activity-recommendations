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

  private[algorithm] def sortGridCells(gridCells: Seq[GridCell]): Seq[GridCell] = {
    gridCells.sortBy(pair => pair.stayPoints.size)(ord = Ordering.Int.reverse)
  }

  private[algorithm] def isNeighbourGridCell(reference: GridCell, possible: GridCell): Boolean = {
    val refX = reference.position._1 // X value (i.e. the mapped longitude)
    val refY = reference.position._2 // Y value (i.e. the mapped latitude)

    val possX = possible.position._1 // X value (i.e. the mapped longitude)
    val possY = possible.position._2 // Y value (i.e. the mapped latitude)

    val conditionX = ((refX - 1) % AlgorithmConfig.NUM_CELLS_LONGITUDE) == possX ||
      (refX % AlgorithmConfig.NUM_CELLS_LONGITUDE) == possX ||
      ((refX + 1) % AlgorithmConfig.NUM_CELLS_LONGITUDE) == possX
    val conditionY = ((refY - 1) % AlgorithmConfig.NUM_CELLS_LATITUDE) == possY ||
      (refY % AlgorithmConfig.NUM_CELLS_LATITUDE) == possY ||
      ((refY + 1) % AlgorithmConfig.NUM_CELLS_LATITUDE) == possY

    possible.assigned = conditionX && conditionY
    conditionX && conditionY
  }
}
