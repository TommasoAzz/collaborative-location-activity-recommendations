package it.unibo.clar
package algorithm.gridcells

import config.AlgorithmConfig

import org.apache.spark.Partitioner

class GridCellPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions
  val numCellPerPartition: Int = AlgorithmConfig.NUM_CELLS_LONGITUDE / numPartitions

  def getPartition(key: Any): Int = key match {
    case gridCell: (Int, Int) => (gridCell._1 / numCellPerPartition) % numPartitions // Approximation
    case _ => 0 // Error case
  }

  override def equals(other: Any): Boolean = other match {
    case h: GridCellPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
