package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.util.Properties

object getOracleData1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getOracleData1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------

    val url ="jdbc:oracle:thin://localhost:1521/orcl"
    /*
    val df = spark.read.format("jdbc").option("url",url).option("user","scott")
      .option("password","tiger").option("driver","oracle.jdbc.OracleDriver").option("dbtable","DEPT").load()
    df.show()
     */

    //its java way of getting the data
    val mprop = new java.util.Properties()
    mprop.setProperty("user","scott")
    mprop.setProperty("password","tiger")
    mprop.setProperty("driver","oracle.jdbc.OracleDriver")

    val mdf = spark.read.jdbc(url,"emp",mprop)
    mdf.show()

    //Write data to oracle database
    mdf.write.mode(SaveMode.Overwrite).format("jdbc").option("url",url).option("user","scott").option("password","tiger")
      .option("driver","oracle.jdbc.OracleDriver").option("dbtable","mydata").save()
    //---------------------------------------------------
    spark.stop()
  }
}