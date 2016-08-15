package com.avnish.patterns

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.util.Utils

/**
  * Created by neha on 8/14/2016.
  */
class CustomHashPartitioner (partitions: Int) extends Partitioner {

  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case (x,y) => x.hashCode%numPartitions
    case _  => 0
  }

  override def equals(other: Any): Boolean = other match {
    case h: CustomHashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
