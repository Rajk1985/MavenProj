package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source

object rdd_operations {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rdd_operations").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------

    val data = "F:\\bigdata\\Dataset\\10000Records.csv" //you can use arg[0] if you want to parameterized
    val drdd = sc.textFile(data)

    drdd.take(5).foreach(println) // take is an action

    val reg = "[^a-zA-Z0-9]"
    val skip_first_line = drdd.first()

    println("First Line---"+skip_first_line)

    val res = drdd.filter(x=>x!=skip_first_line)
                  .flatMap(x=>x.split(" "))
                  .map(x=>(x.replaceAll(reg,""),1)) // removed extra .,#@ using regex
                  .reduceByKey((a,b) => a+b)
                  .sortBy(x=>x._2,false)
    println("----------------------------")
    res.take(10).foreach(println)

    //-----Filter operation On RDD---
    val data1 = "F:\\bigdata\\Dataset\\asl.csv"
    val aslrdd= sc.textFile(data1) // by defualt all element is considered as String in Rdd
    aslrdd.foreach(println)

    // Map is used to apply logic based on Boolean value on top of each element of a tuple on structured data & Filter is used to filter records
    // on certain conditions.

    val skip = aslrdd.first()
    val res1 = aslrdd.filter(x=>x!=skip)             // Skipped header from the data
      .map(x=>x.split(","))     // Seperated the strings of columns by ','
      .map(x=>(x(0), x(1).toInt, x(2))) // converted second column as INT
      .filter(x=>x._3 == "blr")       // Filtered only "blr" rcords
      .filter(x=>x._2 >= 30)          // Filtered only records whose age >= 30

    res1.collect.foreach(println)

   ///--Looping though elemts of RDD without Spark
    val buffrdSrc = Source.fromFile("F:\\bigdata\\Dataset\\asl.csv")

    for (line <- buffrdSrc.getLines){
      val cols= line.split(",").map(_.trim)
      val col1 = cols(0)
      val col2 = cols(1)
      val col3 = cols(2)
      println("Value Col1-->"+col1+"\n"+"Value Col2-->"+col2+"\n"+"Value Col3-->"+col3)
    }
    buffrdSrc.close()

    //Looping through Spark
    for (row <- aslrdd.collect){
       val col1 = row.mkString(",").split(",")(0)
      val col2  = row.mkString(",").split(",")(1)
      val col3  = row.mkString(",").split(",")(2)
    }
    //---------------------------------------------------
    spark.stop()
  }
}