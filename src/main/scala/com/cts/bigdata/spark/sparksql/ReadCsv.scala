package com.cts.bigdata.spark.sparksql
import org.apache.log4j.Logger

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ReadCsv {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ReadCsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    Logger.getLogger("Hello")

    println("Inside the clsss")

    val data = "src/main/resources/us-500.csv"
   val df =  spark.read
      .option("header", "true")
      //.format("csv").load("F:\\bigdata\\Dataset\\us-500.csv")
     .format("csv").load(data)

    df.show()

    df.write.mode(SaveMode.Overwrite).format("csv").save("hdfs://localhost:9000//user/file1/")
    //---------------------------------------------------
    spark.stop()

  }
}