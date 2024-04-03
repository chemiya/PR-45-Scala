package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._


object TotalSpentByCustomerSorted {
  
  /** Convertimos a (customerID, amountSpent) tuplas */
  def extractCustomerPricePairs(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
 

  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // SparkContext
    val sc = new SparkContext("local[*]", "TotalSpentByCustomerSorted")   


    //cargamos datos
    val input = sc.textFile("data/customer-orders.csv")


    //procesamos
    val mappedInput = input.map(extractCustomerPricePairs)

    //acumulamos amount por usuario
    val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )

    //cambiamos orden
    val flipped = totalByCustomer.map( x => (x._2, x._1) )
    val primerasFlipped= flipped.take(5)
    println("Primeras filas de flipped:")
    primerasFlipped.foreach(println)
    println("\n")

    //ordenamos
    val totalByCustomerSorted = flipped.sortByKey()
    
    val results = totalByCustomerSorted.collect()
    

    results.foreach(println)
  }
  
}

