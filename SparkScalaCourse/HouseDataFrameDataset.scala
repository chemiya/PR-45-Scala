package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object HouseDataFrameDataset {
  
  case class RegressionSchema(No: Integer, TransactionDate:Double,HouseAge:Double,
                              DistanceToMRT:Double,NumberConvenienceStores:Integer,Latitude:Double,
                              Longitude:Double,PriceOfUnitArea:Double)


  def main(args: Array[String]) {
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()
      



    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/realestate-reducido.csv")
      .as[RegressionSchema]



    val assembler = new VectorAssembler().
      setInputCols(Array("HouseAge","DistanceToMRT","NumberConvenienceStores")).
      setOutputCol("features")
    val df = assembler.transform(dsRaw)
        .select("PriceOfUnitArea","features")



    // split
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)
    
    // Linear regression
    val lir = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("PriceOfUnitArea")


    // Train
    val model = lir.fit(trainingDF)
    
    // predict
    val fullPredictions = model.transform(testDF).cache()
    


    // Extract
    val predictionAndLabel = fullPredictions.select("prediction", "PriceOfUnitArea").collect()
    
    // print
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    
    // Stop
    spark.stop()

  }
}