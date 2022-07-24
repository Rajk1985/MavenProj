package main.scala.com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._

import org.apache.spark.sql.functions._

object Demo {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Demo").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    println("Hello World")

    Array("destIp","srcIp").par.foreach { i =>
    {
      i match {
        case "destIp" => {
          val primaryDestValues = "SELECT distinct destIp FROM inputValue"
          //println(primaryDestValues)
        }
        case "srcIp" => {
          val primarySrcValues ="SELECT distinct srcIp FROM inputValue"
         // println(primarySrcValues)
        }}

    }
    }
    //---------------------------------------
    //Running Project
    val data = "F:\\bigdata\\Dataset\\OraQry.csv"
    val df  =  spark.read
              .option("header", "true")
              .format("csv").load(data)
    df.show(false)

    val url ="jdbc:oracle:thin://localhost:1521/orcl"

    //Looping through the elements of Df
    for (row <- df.rdd.collect) {
      var tbname = row.mkString(",").split(",")(0)
      var Qry1 =   row.mkString(",").split(",")(1)
      var Qry2 =   row.mkString(",").split(",")(2)

      val qry = "("+ Qry1 +")"

      println(qry)
      val odf = spark.read.format("jdbc")
                .option("url",url)
                .option("user","scott")
                .option("password","tiger")
                .option("driver","oracle.jdbc.OracleDriver")
                .option("dbtable",qry)
                .load()

      odf.show()
    }
    //---------------------------------------------------
    spark.stop()

  }
}