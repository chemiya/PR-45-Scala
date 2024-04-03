package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._


object MostPopularSuperhero {
  
  //cogemos primero y contamos resto
  def countCoOccurrences(line: String): (Int, Int) = {
    val elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
  
  // puede que se retorne elemento o no
  def parseNames(line: String) : Option[(Int, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      Some(fields(0).trim().toInt, fields(1))
    } else None
  }
 

  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // SparkContext
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")   
    
    // con los elementos llamamos funcion
    val names = sc.textFile("data/marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)
    
    // leemos
    val lines = sc.textFile("data/marvel-graph.txt")
    
    // con los elementos llamamos funcion
    val pairings = lines.map(countCoOccurrences)
    
    // combinamos
    val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )
    
    // cambiamos orden
    val flipped = totalFriendsByCharacter.map( x => (x._2, x._1) )
    
    // encontramos maximo
    val mostPopular = flipped.max()
    
    // buscamos mejor
    val mostPopularName = namesRdd.lookup(mostPopular._2).head
    
    // imprimir
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.") 
  }
  
}
