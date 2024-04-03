package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object MostPopularSuperheroDataset {

  //clases
  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)
 

  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession
    val spark = SparkSession
      .builder
      .appName("MostPopularSuperhero")
      .master("local[*]")
      .getOrCreate()

    // schema
    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // leemos datos
    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    // leemos datos
    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    //separamos por espacios el primer elemento, contamos y agrupamos
    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    //ordenamos y mostramos primero
    val mostPopular = connections
        .sort($"connections".desc)
        .first()

    //filtramos y primero
    val mostPopularName = names
      .filter($"id" === mostPopular(0))
      .select("name")
      .first()

    println(s"${mostPopularName(0)} is the most popular superhero with ${mostPopular(1)} co-appearances.")






    //min
    val minConnectionCount = connections
      .agg(min($"connections")).first().getLong(0)

    //elementos enteros
    val minConnection = connections
      .filter($"connections"===minConnectionCount)

    //join por id
    val minConnectionsNames= minConnection.join(names,usingColumn = "id")

    println("The following characters have only"+minConnectionCount+" connections")
    minConnectionsNames.select("name").show()




  }
}
