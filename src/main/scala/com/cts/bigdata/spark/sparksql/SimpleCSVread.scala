package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SimpleCSVread {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SimpleCSVread").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val df =
    //spark.read
    // .option("header", "true")
    // .csv("F:\\Documents\\PLSQL Programs\\Hadoop\\Dataset\\us-500.csv")

      spark.read
        .option("header", "true")
        .format("csv").load("F:\\bigdata\\Dataset\\us-500.csv")

    df.printSchema()
    df.show(2)

    //---------------------------------------------------
    spark.stop()
      }
}