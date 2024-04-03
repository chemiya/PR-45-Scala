package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._


object E1_FriendsByAge {
  

  def parseLine(line: String): (Int, Int) = {
      val fields = line.split(",")
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      (age, numFriends)
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")
    val lines = sc.textFile("data/fakefriends-noheader-reducido.csv")
    val rdd = lines.map(parseLine)

    println("Elementos leidos:")
    rdd.take(5).foreach(println)
    val numeroElementos = rdd.count()
    println(s"Número de elementos en el RDD: $numeroElementos")
    println("\n")

    val totalsByAge = rdd.mapValues(x => (x, 1))
    totalsByAge.take(5).foreach(println)

    val totalsByAgeReduced= totalsByAge.reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    totalsByAgeReduced.take(5).foreach(println)
    println("\n")

    val averagesByAge = totalsByAgeReduced.mapValues(x => x._1 / x._2)
    val results = averagesByAge.collect()
    results.sorted.foreach(println)



  }
    
}
  