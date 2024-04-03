package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object WordCountBetterSortedDataset {

  //clase
  case class Book(value: String)


  def main(args: Array[String]) {

    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // leemos fichero procesandolo
    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

    // buscamos las palabras
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    // ponemos a minusculas
    val lowercaseWords = words.select(lower($"word").alias("word"))

    // agrupamos y contamos
    val wordCounts = lowercaseWords.groupBy("word").count()

    // ordenamos
    val wordCountsSorted = wordCounts.sort("count")

    // mostramos todos
    wordCountsSorted.show(wordCountsSorted.count.toInt)







    // leemos
    val bookRDD = spark.sparkContext.textFile("data/book.txt")

    //filtramos palabras
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = wordsRDD.toDS()

    //convertimos a minusculas
    val lowercaseWordsDS = wordsDS.select(lower($"value").alias("word"))

    //contamos
    val wordCountsDS = lowercaseWordsDS.groupBy("word").count()

    //ordenamos
    val wordCountsSortedDS = wordCountsDS.sort("count")
    wordCountsSortedDS.show(wordCountsSortedDS.count.toInt)

  }
}

