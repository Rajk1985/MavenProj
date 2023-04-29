package com.cts.bigdata.spark.sparkstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object StreamdataOracle {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("StreamdataOracle").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val lines = ssc.socketTextStream("ec2-65-0-80-86.ap-south-1.compute.amazonaws.com", 1234)

    lines.print()

    val res = lines.foreachRDD { a =>
      val spark = SparkSession.builder.config(a.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = a.map(x => x.split(",")).map(x => (x(0), x(1), x(2))).toDF("name", "age", "city")
      df.show()

      //Created Table of Streamed Data
      df.createOrReplaceTempView("tab")
      //use live data in SQL query
      val info = spark.sql("select * from tab")

      //Creating Local Database Oracle instance
      val url ="jdbc:oracle:thin://localhost:1521/orcl"
      val mprop = new java.util.Properties()
      mprop.setProperty("user","scott")
      mprop.setProperty("password","tiger")
      mprop.setProperty("driver","oracle.jdbc.OracleDriver")

      //Write to Oracle database as livedata table
      info.write.mode(SaveMode.Append).jdbc(url,"livedata",mprop)
    }
    ssc.start()
    ssc.awaitTermination()
    //---------------------------------------------------

  }
}