package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object WordCountBetterSorted {
 

  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // SparkContext
    val sc = new SparkContext("local", "WordCountBetterSorted")   
    
    // leemos datos
    val input = sc.textFile("data/book.txt")
    
    // buscamos palabras
    val words = input.flatMap(x => x.split("\\W+"))
    
    // a minusculas
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // contamos ocurrencias cada palabra
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    val primerasWords = wordCounts.take(5)
    println("Primeras filas de words:")
    primerasWords.foreach(println)
    println("\n")
    
    // pasamos (word, count) a (count, word) y ordenamos por key
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()

    
    // imprimimos
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
  
}

