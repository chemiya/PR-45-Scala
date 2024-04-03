package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
    
object DataFramesDataset {

  //creamos clase
  case class Person(id:Int, name:String, age:Int, friends:Int)


  def main(args: Array[String]) {
    
    // log
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // SparkSession
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // leemos datos convirtiendo en la clase
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]


    //imprimimos esquema
    println("Here is our inferred schema:")
    people.printSchema()

    //imprimimos columna
    println("Let's select the name column:")
    people.select("name").show()

    //filtramos
    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()


    //agrupamos
    println("Group by age:")
    people.groupBy("age").count().show()

    //sumamos 10 a los años
    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()
    
    spark.stop()
  }
}