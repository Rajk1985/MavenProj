package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object join {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("join").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------

    val data1 = "F:\\bigdata\\Dataset\\t1.txt"
    val data2 = "F:\\bigdata\\Dataset\\t2.txt"
    val df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data1).withColumn("id",monotonically_increasing_id())
    val df2 = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data2).withColumn("uid",monotonically_increasing_id())

    df1.show()
    df2.show()
    //this is sql way
    df1.createOrReplaceTempView("tab1")
    df2.createOrReplaceTempView("tab2")

    val res = spark.sql("Select * from tab1 join tab2 on tab1.id = tab2.uid ").drop("uid")
    res.show()

    //This is dsl way
    val join = df1.join(df2,$"id"===$"uid","inner").drop("uid")
    join.show()

    //---------------------------------------------------
    spark.stop()
  }
}