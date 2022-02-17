package it.unibo.clar
package algorithm.gridcells

import config.AlgorithmConfig

import org.apache.spark.Partitioner

class GridCellPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  //val numCellPerPartition: Int = AlgorithmConfig.NUM_CELLS_LONGITUDE / numPartitions // Use gridCell._1 in getPartition
  val numCellPerPartition: Int = AlgorithmConfig.NUM_CELLS_LATITUDE / numPartitions // Use gridCell._2 in getPartition

  def getPartition(key: Any): Int = key match {
    case gridCell: (Int, Int) =>
      val partition = gridCell._2 / numCellPerPartition
      if (partition == numPartitions) numPartitions - 1 else partition
    //(gridCell._2 / numCellPerPartition) % numPartitions // Approximation and slower
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
