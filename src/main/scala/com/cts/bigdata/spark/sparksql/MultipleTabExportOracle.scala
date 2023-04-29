package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object MultipleTabExportOracle {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("MultipleTabExportOracle").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val url = "jdbc:oracle:thin://localhost:1521/orcl"

    //its java way of getting the data
    val mprop = new java.util.Properties()
    mprop.setProperty("user", "scott")
    mprop.setProperty("password", "tiger")
    mprop.setProperty("driver", "oracle.jdbc.OracleDriver")

    //---- Reading 2 tables from Datase ---
    //Create array of Tables
    val tabls = Array("EMP", "DEPT")

    tabls.foreach { x =>
      val mdf = spark.read.jdbc(url, s"$x", mprop)
      //----Insert records to Hive
      //mdf.write.mode(SaveMode.Append).format("hive").saveAsTable(s"$x")
      mdf.show() }
    //-------------------------------------------------------
   //---------Reading All Table from Database Schema----------
    val qry = "(Select table_name from all_tables where owner = 'SCOTT') t"
    val all = spark.read.jdbc(url,qry,mprop).rdd.map(x=>x(0)).collect.toList

    val tabs = all

    tabs.foreach({x=>
      println(s"importing $x table")
      val mdf = spark.read.jdbc(url,s"$x",mprop)

      //----Store Data either to Hive or MySQl
      //mdf.write.mode(SaveMode.Overwrite).jdbc(myurl,s"$x", mprop)
      //mdf.write.mode(SaveMode.Append).format("hive").saveAsTable(s"$x")

      println(s"successfully imported $x table")
      mdf.show()

      })

    //---------------------------------------------------
    spark.stop();
  }
}
