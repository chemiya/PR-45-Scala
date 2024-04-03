package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer


object DegreesOfSeparation {
  
  // para buscar
  val startCharacterID = 5306
  val targetCharacterID = 14
  
  // acumulador global
  var hitCounter:Option[LongAccumulator] = None
  
  // tipos creados
  // ID connections, distance, color.
  type BFSData = (Array[Int], Int, String)
  // heroID y el elemento asociado
  type BFSNode = (Int, BFSData)
    

  def convertToBFS(line: String): BFSNode = {
    
    // separamos campos
    val fields = line.split("\\s+")
    
    // id
    val heroID = fields(0).toInt
    
    // extraemos resto ids
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for ( connection <- 1 until (fields.length - 1)) {
      connections += fields(connection).toInt
    }
    
    // valores defecto
    var color:String = "WHITE"
    var distance:Int = 9999
    
    // en el que iniciamos
    if (heroID == startCharacterID) {
      color = "GRAY"
      distance = 0
    }
    
    (heroID, (connections.toArray, distance, color))
  }
  

  def createStartingRdd(sc:SparkContext): RDD[BFSNode] = {
    //leemos
    val inputFile = sc.textFile("data/marvel-graph.txt")
    //convertimos a estructura
    inputFile.map(convertToBFS)
  }
  

  def bfsMap(node:BFSNode): Array[BFSNode] = {
    
    // extraemos campos
    val characterID:Int = node._1
    val data:BFSData = node._2
    
    val connections:Array[Int] = data._1
    val distance:Int = data._2
    var color:String = data._3
    

    var results:ArrayBuffer[BFSNode] = ArrayBuffer()
    
  //si es nodo gris
    if (color == "GRAY") {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = "GRAY"
        
        // si llegamos al que queremos
        if (targetCharacterID == connection) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }
        
        // creamos nuevo
        val newEntry:BFSNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }
      
      // ponemos color
      color = "BLACK"
    }
    
    // lo añadimos
    val thisEntry:BFSNode = (characterID, (connections, distance, color))
    results += thisEntry
    
    results.toArray
  }
  

  def bfsReduce(data1:BFSData, data2:BFSData): BFSData = {
    
    // extraemos datos
    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    val distance1:Int = data1._2
    val distance2:Int = data2._2
    val color1:String = data1._3
    val color2:String = data2._3
    
    // valores por defecto
    var distance:Int = 9999
    var color:String = "WHITE"
    var edges:ArrayBuffer[Int] = ArrayBuffer()
    
    // vemos si es el original
    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }
    
    // guardamos minima distancia
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }
    
    // guardamos color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }
	if (color1 == "GRAY" && color2 == "GRAY") {
	  color = color1
	}
	if (color1 == "BLACK" && color2 == "BLACK") {
	  color = color1
	}
    
    (edges.toArray, distance, color)
  }
    

  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // SparkContext
    val sc = new SparkContext("local[*]", "DegreesOfSeparation") 
    
    // Oaccumulator
    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    //cargamos datos
    var iterationRdd = createStartingRdd(sc)

    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)
   
      //aplicamos funcion
      val mapped = iterationRdd.flatMap(bfsMap)
      
      //cuenta
      println("Processing " + mapped.count() + " values.")
      
      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount + 
              " different direction(s).")
          return
        }
      }
      
      // reducimos
      iterationRdd = mapped.reduceByKey(bfsReduce)
    }
  }
}