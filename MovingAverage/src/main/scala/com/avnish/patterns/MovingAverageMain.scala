package com.avnish.patterns

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, MutableList}

/**
  * Created by neha on 8/14/2016.
  */
object MovingAverageMain {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage : OrderInversion <input> <output> <windowSize>")
      System.exit(-1)
    }
    val inputFile = args(0)
    val outputFile = args(1)
    val windowSize: Int = args(2) toInt
    val conf = new SparkConf().setAppName("orderInversion").setMaster("local[2]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile(inputFile)

    input.foreach(println)
  }
}

