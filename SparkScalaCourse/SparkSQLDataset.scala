package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLDataset {



  //clase
  case class Person(id:Int, name:String, age:Int, friends:Int)


  def main(args: Array[String]) {
    
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // SparkSession
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // leemos csv con elementos como clase
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    schemaPeople.printSchema()

    //creamos vista sql
    schemaPeople.createOrReplaceTempView("people")


    //filtramos
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)
    
    spark.stop()
  }
}