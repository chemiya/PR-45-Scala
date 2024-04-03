package com.sundogsoftware.spark

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object MinTemperaturesDataset {

  //clase
  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)


  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession
    val spark = SparkSession
      .builder
      .appName("MinTemperatures")
      .master("local[*]")
      .getOrCreate()


    //definimos estructura
    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    // leemos datos
    import spark.implicits._
    val ds = spark.read
      .schema(temperatureSchema)
      .csv("data/1800.csv")
      .as[Temperature]
    
    // filtramos por campo
    val minTemps = ds.filter($"measure_type" === "TMIN")
    
    // seleccionamos atributos
    val stationTemps = minTemps.select("stationID", "temperature")
    
    // agrupamos y cogemos menor
    val minTempsByStation = stationTemps.groupBy("stationID").min("temperature")

    // convertimos a fahrenheim y ordenamos
    val minTempsByStationF = minTempsByStation
      .withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
      .select("stationID", "temperature").sort("temperature")

    // Collect
    val results = minTempsByStationF.collect()


    //imprimimos
    for (result <- results) {
       val station = result(0)
       val temp = result(1).asInstanceOf[Float]
       val formattedTemp = f"$temp%.2f F"
       println(s"$station minimum temperature: $formattedTemp")
    }
  }
}