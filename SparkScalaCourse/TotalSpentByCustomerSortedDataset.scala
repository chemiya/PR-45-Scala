package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}


object TotalSpentByCustomerSortedDataset {

  //clase
  case class CustomerOrders(cust_id: Int, item_id: Int, amount_spent: Double)


  def main(args: Array[String]) {
   
    // logs
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("TotalSpentByCustomer")
      .master("local[*]")
      .getOrCreate()

    // schema
    val customerOrdersSchema = new StructType()
      .add("cust_id", IntegerType,nullable = true)
      .add("item_id", IntegerType,nullable = true)
      .add("amount_spent", DoubleType,nullable = true)


   //cargamos datos
    import spark.implicits._
    val customerDS = spark.read
      .schema(customerOrdersSchema)
      .csv("data/customer-orders.csv")
      .as[CustomerOrders]

    //agrupamos y calculamos suma
    val totalByCustomer = customerDS
      .groupBy("cust_id")
      .agg(round(sum("amount_spent"), 2)
        .alias("total_spent"))


    //ordenamos
    val totalByCustomerSorted = totalByCustomer.sort("total_spent")


    //mostramos valor
    totalByCustomerSorted.show(totalByCustomer.count.toInt)
  }
  
}

