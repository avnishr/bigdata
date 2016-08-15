package com.avnish.patterns

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, MutableList}

/**
  * Created by neha on 8/14/2016.
  */
object OrderInversionMain {

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

    val rdd2 = input.flatMap(x => splitIntoPairs(x, windowSize)).map( x => (x,1))
    val rdd3 = rdd2.partitionBy(new CustomHashPartitioner(input.partitions.length)).reduceByKey( (a,b) => (a+b)).sortByKey()
    rdd3.foreach(println)
  }

  def splitIntoPairs(row: String, width: Int): List[(String, String)] = {
    val words: Array[String] = row.split(" ")
    var retList = scala.collection.mutable.ListBuffer[(String, String)]()
    var i: Int = 0;
    println("The words array size is " + words.length)
    while (i < words.length) {
      var end: Int = 0;
      if (i + width < words.length) end = i + width else end = words.length-1;
      for (j <- i+1 to end) {
        var tuple: (String, String) = (words(i), words(j))
        retList += tuple
        tuple = (words(j), words(i))
        retList += tuple
      }
      i = i+1
    }
    retList toList
  }


}

