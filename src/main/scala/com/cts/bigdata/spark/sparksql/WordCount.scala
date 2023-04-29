package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WordCount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("WordCount").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    //This program find the occurance of a word in an unstructured data

    val data = "F:\\bigdata\\Dataset\\10000Records.csv" //you can use arg[0] if you want to parameterized

    val drdd = sc.textFile(data)

    drdd.take(5).foreach(println)
    //Flatmap - Apply Logix on top of each and every element and then flatten the result
    //Input Output element length is not same i.e 3 input elemnt output can be 10 element

    val reg = "[^a-zA-Z0-9]"
    val res = drdd.flatMap(x=>x.split(" "))
                  .map(x=>(x.replaceAll(reg,""),1)) // removed extra .,#@ using regex
                  .reduceByKey((a,b) => a+b)
                  .sortBy(x=>x._2,false)

    //res.collect.foreach(println)
    res.take(5).foreach(println) // use take(n) to print specific no. of lines


    //---------------------------------------------------
    spark.stop()
  }
}