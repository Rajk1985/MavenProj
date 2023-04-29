package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object rddsparkpoc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddsparkpoc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val data = "F:\\bigdata\\Dataset\\bank-full.csv"
    val  brdd   = sc.textFile(data) //sc.texfile is used to create the RDD

    val skip = brdd.first()

    val res = brdd.filter(x=>x!=skip)
              .map(x=>x.replaceAll("\"",""))
              .map(x=>x.split(";"))
              .map(x=>(x(0),x(1),x(2),x(3),x(4)))
               .filter(x=>x._1.toInt>80 && x._3 =="married" && x._4=="secondary")
               .sortBy(x=>x._1,false)

        res.take(5).foreach(println)
    //---------------------------------------------------
    spark.stop()
  }
}