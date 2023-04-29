/****************************************************
 *This objective of this code is to demonstrate calling of a
 * method from Main.
 * **************************************************/

package com.cts.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ReadCsv {
  //Example of Main calling a function
  def main(args: Array[String]){
    val df = readdata
    df.show()
  }
  // readdat is a method
  def readdata  = {
    val spark = SparkSession.builder.master("local[*]").appName("myaname").getOrCreate()
    // this will generate a Dataframe
      spark.read
        .option("header", "true")
        .format("csv").load("F:\\bigdata\\Dataset\\us-500.csv")
  }
}
