package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object complexjson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexjson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------

    val data = "F:\\bigdata\\Dataset\\world_bank.json"
    val df = spark.read.format("json").load(data)

    df.show()
    df.printSchema()

    //If you have array then use "Explode"
    //If yu have Struct then use parentCol.ChildColumn eg project_abstract

    val ndf = df.withColumn("tn",explode($"theme_namecode")).withColumn("theme1name",$"theme1.name")
      .withColumn("theme1Percent",$"theme1.Percent")
      //.select($"*",$"tn.*")
      .withColumn("sn",explode($"sector_namecode"))
     // .select($"*",$"sn.*")
      .withColumn("prjdc",explode($"projectdocs"))
      .select($"*",$"sn.*",$"*",$"tn.*",$"*",$"prjdc.*",$"project_abstract.cdata")
      .drop("projectdocs","project_abstract","theme_namecode")
       ndf.show(3)
       ndf.printSchema()

       //Once whole data gets normalized in normal datatpe it can be saved to database
    //---------------------------------------------------
    spark.stop()
  }
}