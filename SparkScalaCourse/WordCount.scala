package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._


object WordCount {
 

  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // SparkContext
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // leer datos
    val input = sc.textFile("data/book.txt")
    
    // separar por espacios
    val words = input.flatMap(x => x.split(" "))
    val primerasWords = words.take(5)
    println("Primeras filas de words:")
    primerasWords.foreach(println)
    println("\n")
    
    // contamos cada palabra
    val wordCounts = words.countByValue()
    
    // imprimimos
    wordCounts.foreach(println)
  }
  
}

