package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RDDMapFilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("RDDMapFilter").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val data = "F:\\bigdata\\Dataset\\asl.csv"
    val aslrdd= sc.textFile(data) // by defualt all element is considered as String in Rdd

    // Map is used to apply logic based on Boolean value on top of each element of a tuple on structured data & Filter is used to filter records
    // on certain conditions.

    val skip = aslrdd.first()
    val res = aslrdd.filter(x=>x!=skip)             // Skipped header from the data
                    .map(x=>x.split(","))     // Seperated the strings of columns by ','
                    .map(x=>(x(0), x(1).toInt, x(2))) // converted second column as INT
                    .filter(x=>x._3 == "blr")       // Filtered only "blr" rcords
                    .filter(x=>x._2 >= 30)          // Filtered only records whose age >= 30

    res.collect.foreach(println)

    println("------------------------------------------------------")

    //To implement GroupBy on a column we use ReduceBykey(key,val). This functions combines result on value
    val res1 = aslrdd.filter(x=>x!=skip)             // Skipped header from the data
                     .map(x=>x.split(","))    // Converted string to tuple since by default RDD is String
                     .map(x=>(x(2),1)).reduceByKey((a,b) => a+b)  // ReduceByKey works on concept of (Key,Value)
                     .sortBy(x=>x._2,false)  // This is used to ascending or decending order

    res1.collect.foreach(println)
    //---------------------------------------------------
    spark.stop()
  }
}