package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object complexcsv {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexcsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data = "F:\\bigdata\\Dataset\\10000Records.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    //df.show() // by defualt it shows 20 records.
             // if you want to display no. of rows just mention rownumber
    df.show(2, false)  // By default long srings are truncated by ... for eg http://www.chapma...

    //if you have data with Spaces / Special Char . remove them using in SQL query else parser will fail

    val reg = "[^a-zA-Z0-9]"
    //val cols = df.columns //--<-- Used to display the columns of the dataframe

    //used to replace Special char
    val cols = df.columns.map(x=>x.replaceAll(reg,""))

    //This is used to append clean data to existing dataframe
    //toDF is used to rename all columns and to convert RDD to dF

    // val all = Array("Empid","Firstname","lastname")
    //val ndf = df.toDF(all:_*) this same as val ndf = df.toDF("Empid","Firstname","lastname")

    //val ndf = df.toDF(cols:_*)

    //To put date in specific format use Format option
    val ndf = df.toDF(cols:_*).withColumn("DateofBirth",to_date($"DateofBirth","MM/dd/yyyy"))

    //Putting $ infront of columnname will make spark understandng that its nt a string bt a columnname
    ndf.select($"empid",$"email").show(2,false)
    ndf.select($"empid",$"email").where ($"email".like("%gmail%")).show(5,false)
    ndf.groupBy($"empid").mean().show(2)

    //to virtually drop column from a dataframe not from original data
    val ndf1 = ndf.drop("email")

    ndf.createTempView("tab")
    val res = spark.sql ("select * from tab" )

    res.show(5,false)

    spark.stop()
  }
}