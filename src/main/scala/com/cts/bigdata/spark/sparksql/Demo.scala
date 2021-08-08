package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Demo {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Demo").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------

     println("Hello World")

    //---------------------------------------------------
    spark.stop()
    //Start Streaming
    //ssc.start()
    //ssc.awaitTermination() //Dont terminate session
  }
}