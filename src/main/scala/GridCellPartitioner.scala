package it.unibo.clar

import org.apache.spark.Partitioner

class GridCellPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions
  val numCellPerPartition: Int = Config.NUM_CELLS_LONGITUDE / numPartitions

  def getPartition(key: Any): Int = {
    val gridCell = key.asInstanceOf[(Int, Int)]
    (gridCell._1 / numCellPerPartition) % numPartitions // Approximation
  }

  override def equals(other: Any): Boolean = other match {
    case h: GridCellPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
