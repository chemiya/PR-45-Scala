package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._


object TotalSpentByCustomer {
  
  /** convertimos a (customerID, amountSpent) tuplas */
  def extractCustomerPricePairs(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
 

  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // SparkContext
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")   

    //carmos datos
    val input = sc.textFile("data/customer-orders.csv")


    //procesamos
    val mappedInput = input.map(extractCustomerPricePairs)


    //sumamos amount por usuario
    val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )
    
    val results = totalByCustomer.collect()
    

    results.foreach(println)
  }
  
}

