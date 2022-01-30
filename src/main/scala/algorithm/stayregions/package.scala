package it.unibo.clar
package algorithm

import config.{AlgorithmConfig, SparkProjectConfig}
import model.{GridCell, StayRegion}

import algorithm.gridcells.GridCellPartitioner
import algorithm.stayregions.Partitionings.Partitioning
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer


package object stayregions {
  def computeStayRegions(gridCells: RDD[GridCell])(partitioning: Partitioning): RDD[StayRegion] = {
    // val neighbourRatios = new ListBuffer[Seq[Double]]

    val results = partitioning match {
      case Partitionings.GridCell => gridCells
        .map(gc => (gc.position, gc))
        .partitionBy(new GridCellPartitioner(SparkProjectConfig.DEFAULT_PARALLELISM))
        .mapPartitions(pairs => {
          val result = _computeStayRegions(pairs.map(_._2))
          // neighbourRatios += result._2
          // println(s"Partition mean neighbour ratio: ${result._2.sum / result._2.size} (${result._2.sum}, ${result._2.size})")
          result
        })
      case Partitionings.Hash => gridCells
        .mapPartitions(cells => {
          val result = _computeStayRegions(cells)
          // neighbourRatios += result._2
          result
        })
    }

    // val ratios = neighbourRatios.filter(_.nonEmpty).map(ratios => ratios.sum / ratios.size)
    val nonNullRatios = results.map(_._2).filter(_ >= 0.0)
    val meanRatio = nonNullRatios.sum / nonNullRatios.count

    println(s"Mean neighbour ratios: $meanRatio")

    results.map(_._1)
  }

  private def _computeStayRegions(gridCells: Iterator[GridCell]): Iterator[(StayRegion, Double)] = {
    val sortedCells = gridcells.sortGridCells(gridCells.toSeq)

    if (sortedCells.isEmpty) {
      (Iterator(), Seq())
    }

    val stayRegionsAndRatios = (for {
      i <- sortedCells.indices
      if !sortedCells(i).assigned // if not already assigned to a stay region
    } yield computeStayRegion(i, sortedCells)).filter(_._3 >= AlgorithmConfig.MIN_NUM_STAY_POINTS_PER_REGION)

    // (stayRegionsAndRatios.map(_._1).iterator, stayRegionsAndRatios.map(_._2).iterator)
    stayRegionsAndRatios.map(sr => (sr._1, sr._2)).iterator
  }

  private def computeStayRegion(index: Int, gridCells: Seq[GridCell]): (StayRegion, Double, Int) = {
    val reference = gridCells(index)
    val neighbours = gridCells.filter(gridcells.isNeighbourGridCell(reference, _))

    val neighbouringStayPoints = neighbours.flatMap(_.stayPoints)

    (new StayRegion(stayPoints = neighbouringStayPoints), neighbours.size / 9.0, neighbouringStayPoints.size)
  }
}
