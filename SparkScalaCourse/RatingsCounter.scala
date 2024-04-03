package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._


object RatingsCounter {
 

  def main(args: Array[String]) {
   
    //logs
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // SparkContext
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // leer datos
    val lines = sc.textFile("data/ml-100k/u.data")
    
    // separamos por tab y cogemos elemento 2
    // formato userID, movieID, rating, timestamp
    val ratings = lines.map(x => x.split("\t")(2))
    println(ratings.getClass)
    val primerasFilasRatings = ratings.take(5)
    println("Primeras filas de Ratings:")
    primerasFilasRatings.foreach(println)
    
    // contamos cuantos de cada uno
    val results = ratings.countByValue()
    println(results.getClass)
    val primerasFilasResults = results.take(5)
    println("Primeras filas de results:")
    primerasFilasResults.foreach(println)
    
    // mostramos resultados ordenados
    val sortedResults = results.toSeq.sortBy(_._1)
    
    // cada uno se imprime en una linea
    sortedResults.foreach(println)
  }
}
