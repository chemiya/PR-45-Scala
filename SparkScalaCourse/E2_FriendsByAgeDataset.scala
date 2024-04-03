package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object E2_FriendsByAgeDataset {


  case class FakeFriends(id: Int, name: String, age: Int, friends: Long)


  def main(args: Array[String]) {
   

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder
      .appName("FriendsByAge")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends-reducido.csv")
      .as[FakeFriends]

    val tipoVariable = ds.getClass.getName
    println(s"El tipo de la variable es: $tipoVariable")

    ds.take(5).foreach(println)
    println("\n")


    val friendsByAge = ds.select("age", "friends")
    friendsByAge.take(5).foreach(println)
    println("\n")


    println("Agrupando por edad:")
    friendsByAge.groupBy("age").avg("friends").show()
    println("\n")

    println("Ordenando:")
    friendsByAge.groupBy("age").avg("friends").sort("age").show()
    println("\n")

    println("Ordenando con redondeo:")
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2))
      .sort("age").show()
    println("\n")

    println("Ordenando con redondeo con nombre exacto:")
    friendsByAge.groupBy("age").agg(round(avg("friends"), 2)
      .alias("friends_avg")).sort("age").show()
    println("\n")
  }
}
  