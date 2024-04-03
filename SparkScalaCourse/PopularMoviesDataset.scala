package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}


object PopularMoviesDataset {

  //creamos la clase
  final case class Movie(movieID: Int)


  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // SparkSession
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    // Crear schema
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._

    // leemos datos
    val moviesDS = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movie]
    
    // agrupamos contamos y ordenamos
    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count"))

    // contamos 10 mejores
    topMovieIDs.show(10)

    // Stop the session
    spark.stop()
  }
  
}

