package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object CreateDF_from_RDD {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("CreateDF_from_RDD").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val LoginActivityTypeId = 0
    val LogoutActivityTypeId = 1

    val readUserData =
      Array(
        (1, "Elmon, Patrick"),
        (2, "Mathew, John"),
        (3, "X, Mr."))

    //Creating RDD using parallelize
    val rdd1 = sc.parallelize(readUserData)
    //Converting to dataframe using toDF
    val df1 = spark.createDataFrame(rdd1).toDF("id","Name")

    val readUserActivityData =
      Array(
        (1, LoginActivityTypeId, 1514764800000L),
        (2, LoginActivityTypeId, 1514808000000L),
        (1, LogoutActivityTypeId,1514829600000L),
        (1, LoginActivityTypeId, 1514894400000L))

    val rdd2 = sc.parallelize(readUserActivityData)
    val df2 = spark.createDataFrame(rdd2).toDF("id","Activity","TimeStamp")

    //df1.show()
    //df2.show()

    df1.createOrReplaceTempView("tab1")
    df2.createOrReplaceTempView("tab2")

    val res = spark.sql("SELECT a.Name , max(b.TimeStamp) from tab1 a join tab2 b on a.id =b.id group by a.name")

    res.show()

    //---------------------------------------------------
    spark.stop()
    //Start Streaming
    //ssc.start()
    //ssc.awaitTermination() //Dont terminate session
  }
}