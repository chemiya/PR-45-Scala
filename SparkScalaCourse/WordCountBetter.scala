package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._


object WordCountBetter {
 

  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // SparkContext
    val sc = new SparkContext("local[*]", "WordCountBetter")   
    
    // leemos datos
    val input = sc.textFile("data/book.txt")
    
    // expresion regular para separar palabras
    val words = input.flatMap(x => x.split("\\W+"))
    val primerasWords = words.take(5)
    println("Primeras filas de words:")
    primerasWords.foreach(println)
    println("\n")
    
    // todo a minusculas
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // contamos
    val wordCounts = lowercaseWords.countByValue()
    
    // imprimimos
    wordCounts.foreach(println)
  }
  
}

