package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkcassandra {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkcassandra").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here------------------------
    //val tab1 = args(0)
    //val tab2 = args(1)

    val adf = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace","rkdb")
      .option("table","asl").load()

    val ndf = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "rkdb")
      .option("table", "nep")
      .load()

    ndf.show(5)
    adf.show(5)

    adf.createOrReplaceTempView("asl")
    ndf.createOrReplaceTempView("nep")

    val res = spark.sql("select a.*, n.phone, n.email from asl a join nep n on a.name=n.name")
    res.show()

    //Write results to rkdb - make sure you already have the table created in cassandra
    res.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .option("keyspace", "rkdb").option("table", "asljoinnep").save()

    //---------------------------------------------------
    spark.stop()
  }
}