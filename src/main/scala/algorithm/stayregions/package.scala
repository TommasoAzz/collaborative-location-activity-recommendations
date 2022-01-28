package it.unibo.clar
package algorithm

import config.{AlgorithmConfig, SparkProjectConfig}
import model.{GridCell, StayRegion}

import algorithm.gridcells.GridCellPartitioner
import algorithm.stayregions.Partitionings.Partitioning
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer


package object stayregions {
  def computeStayRegions(partioning: Partitioning)(gridCells: RDD[GridCell]): RDD[StayRegion] = {
    val neighbourRatios = new ListBuffer[Double]

    val results = partioning match {
      case Partitionings.GridCell => gridCells
        .map(gc => (gc.position, gc))
        .partitionBy(new GridCellPartitioner(SparkProjectConfig.DEFAULT_PARALLELISM))
        .mapPartitions(pairs => {
          val result = _computeStayRegions(pairs.map(_._2))
          neighbourRatios ++= result._2
          result._1
        })
      case Partitionings.Hash => gridCells
        .mapPartitions(cells => {
          val result = _computeStayRegions(cells)
          neighbourRatios ++= result._2
          result._1
        })
    }

    println(s"Mean neighbour ratio: ${neighbourRatios.sum / neighbourRatios.size}")

    results
  }

  private def _computeStayRegions(gridCells: Iterator[GridCell]): (Iterator[StayRegion], Seq[Double]) = {
    val sortedCells = algorithm.gridcells.sortGridCells(gridCells.toSeq)

    if (sortedCells.isEmpty) {
      (Iterator(), Seq())
    }

    val stayRegionsAndRatios = (for {
      i <- sortedCells.indices
      if !sortedCells(i).assigned // if not already assigned to a stay region
    } yield computeStayRegion(i, sortedCells)).filter(_._3 >= AlgorithmConfig.MIN_NUM_STAY_POINTS_PER_REGION)

    (stayRegionsAndRatios.map(_._1).iterator, stayRegionsAndRatios.map(_._2))
  }

  private def computeStayRegion(index: Int, gridCells: Seq[GridCell]): (StayRegion, Double, Int) = {
    val reference = gridCells(index)
    val neighbours = gridCells.filter(isNeighbourGridCell(reference, _))

    val neighbouringStayPoints = neighbours.flatMap(_.stayPoints)

    (new StayRegion(stayPoints = neighbouringStayPoints), neighbours.size / 9.0, neighbouringStayPoints.size)
  }

  private def isNeighbourGridCell(reference: GridCell, possible: GridCell): Boolean = {
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
