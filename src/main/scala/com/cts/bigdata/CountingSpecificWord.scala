package com.cts.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object CountingSpecificWord {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("CountingSpecificWord").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------

    val data = "F:\\bigdata\\Dataset\\intro.txt"
    println("------Creating  RDD --------")
    val rdd = sc.textFile(data)

    rdd.foreach(println)

    //Convert RDD to Dataframe
    val df = rdd.toDF("text")

    //DSL way count occurance of word Raj
    val word = df.filter(col("text").like("%Raj%"))
    word.count() //Dispays count

    //SQL Way
    df.createOrReplaceTempView("tab")
    val res = spark.sql("Select count(1) from tab where text like '%Raj%'")
    res.show()

    println("------Creating direct Dataframe --------")
    val df1 = spark.read.format("csv").option("header","true").load(data)

    df.createOrReplaceTempView("tab1")
    val res1 = spark.sql("Select count(1) from tab1 where text like '%Raj%'")
    res1.show()

    //---------------------------------------------------
    spark.stop()
    //Start Streaming
    //ssc.start()
    //ssc.awaitTermination() //Dont terminate session
  }
}