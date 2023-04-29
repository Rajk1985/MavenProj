package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object csvdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvdata").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data = "F:\\bigdata\\Dataset\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
     df.show()

    //Running SQL Queries
    df.createTempView("tab")
    //val res = spark.sql ("select * from tab where state = 'NJ'" )
    // val res = spark.sql("select state, count(*) cnt from tab group by state order by cnt desc")
    //val res = df.select("*").where($"state"==="NJ" && $"email".like("%gmail.com%"))
    //val res = df.groupBy($"state").agg(count("*").alias("cnt")).orderBy($"cnt".desc)

    //concat_ws --<-- concat with seperator  SQL Way
    val res = spark.sql("Select concat_ws(' ',first_name,last_name) fullname,* from tab where state = 'NJ'")
    res.show()

    //concat_ws DSL way

    val res1 = df.withColumn("FullName",concat_ws(" ",$"first_name",$"last_name",$"State"))
    res1.show()

    //Store data as hive table .Default format of hive will be taken. If you want any specfic format write that inplace of Hive
    res1.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("tabcsvdata")

    spark.stop()
  }
}