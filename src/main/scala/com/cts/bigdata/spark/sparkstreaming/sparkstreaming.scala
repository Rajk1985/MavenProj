package com.cts.bigdata.spark.sparkstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//Import this while doing streaming Also add sparkstreaming.jar in dependencies
import org.apache.spark.streaming._

import java.util.Properties

object sparkstreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkstreaming").getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10)) // Specify the time to feed Live data

    // set the Log to display only error
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    import spark.sql

    //----------Write Logic Here--------------------------
    // Get the live feed from EC2 machine terminal/ use "nc -lk 1234" where 1234 is your port/ Add 1234 to security grp as customTCP
    val lines = ssc.socketTextStream("ec2-65-0-80-86.ap-south-1.compute.amazonaws.com", 1234)

    lines.print() // Print the live data

    //Convert the streaming data to Dataframe for processing
      val res = lines.foreachRDD{ a=>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(a.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      // spark streaming by default generate rdd.... rdd convert to datarame
      //Map columns names
      val df = a.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.show()  // Show the result

      df.createOrReplaceTempView("tab")
        //use live data in SQL query
        val masinfo = spark.sql("select * from tab where city='mas'")

      //Created Mysql instance on AWS to store live data to table
      val url ="jdbc:mysql://testingmysql.ci1i74maitki.ap-south-1.rds.amazonaws.com:3306/mysqldb"
      val prop = new Properties()
      prop.setProperty("user","musername")
      prop.setProperty("password","mpassword")
      prop.setProperty("driver","com.mysql.jdbc.Driver")

      //Write to mysql database as livemas table
      masinfo.write.mode(SaveMode.Append).jdbc(url,"livemas",prop)

    }
    //Start Streaming
    ssc.start()
    ssc.awaitTermination() //Dont terminate session
    //---------------------------------------------------

  }
}