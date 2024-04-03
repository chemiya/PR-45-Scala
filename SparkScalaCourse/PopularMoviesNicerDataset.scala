package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}


object PopularMoviesNicerDataset {

  //clase
  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)


  def loadMovieNames() : Map[Int, String] = {

    // para interpretar los datos
    implicit val codec: Codec = Codec("ISO-8859-1")

    // creamos un mapa
    var movieNames:Map[Int, String] = Map()

    //leemos
    val lines = Source.fromFile("data/ml-100k/u.item")
    for (line <- lines.getLines()) {
      //separamos
      val fields = line.split('|')
      if (fields.length > 1) {
        //añadimos al mapa
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }


  def main(args: Array[String]) {
   
    // log
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession
    val spark = SparkSession
      .builder
      .appName("PopularMoviesNicer")
      .master("local[*]")
      .getOrCreate()

    //llamamos funcion
    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    // schema
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    // leemos
    import spark.implicits._
    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    // contamos agrupando por id
    val movieCounts = movies.groupBy("movieID").count()

    //que busque por id el nombre
    val lookupName : Int => String = (movieID:Int)=>{
      nameDict.value(movieID)
    }

    //utilizamos udf
    val lookupNameUDF = udf(lookupName)

    // añadimos nueva columna procesando el campo nuevo
    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))

    // ordenamos
    val sortedMoviesWithNames = moviesWithNames.sort("count")

    // mostramos todo
    sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate = false)
  }
}

