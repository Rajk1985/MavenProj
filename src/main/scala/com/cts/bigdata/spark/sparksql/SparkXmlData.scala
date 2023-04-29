package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkXmlData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkXmlData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    //Read XMl file
    val data = "F:\\bigdata\\Dataset\\books.xml"
    val df = spark.read.format("xml").option("rowTag","book")
             .load(data)
    df.show()

    df.createTempView("tab")

    //val res = spark.sql("Select author,count(*) cnt from tab group by author")
    val res = spark.sql("Select * from tab where genre = 'Fantasy'")
    res.show()

    //----Write the data to Different Formats

    res.write.mode(SaveMode.Overwrite).format("avro").save("F:\\bigdata\\Dataset\\output\\booksAvro")
    res.write.mode(SaveMode.Overwrite).format("orc").save("F:\\bigdata\\Dataset\\output\\booksOrc")
    res.write.mode(SaveMode.Overwrite).format("parquet").save("F:\\bigdata\\Dataset\\output\\booksparq")

    //-----------------------
    println("---Testing Data---")
    val data1 = "F:\\bigdata\\Dataset\\booksOrc"
    println("---Orc Data--")
    val df1 = spark.read.format("orc").load("F:\\bigdata\\Dataset\\booksOrc")
    df1.show()

    println("---Avro Data--")
    val df2 = spark.read.format("avro").load("F:\\bigdata\\Dataset\\booksAvro")
    df2.show()

    println("---Parquet Data--")
    val df3 = spark.read.format("parquet").load("F:\\bigdata\\Dataset\\output\\booksparq")
    df3.show()

    //---------------------------------------------------
    spark.stop()
  }
}