package com.cts.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DateFunc {
  def main(args: Array[String]) {
    //config("spark.sql.session.timeZone","IST") --> added to adjust dates as per timeZone
    val spark = SparkSession.builder.master("local[*]").config("spark.sql.session.timeZone","IST").appName("complexcsv2").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------

    val data = "F:\\bigdata\\Dataset\\10000Records.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    val reg = "[^a-zA-Z0-9]"
    //val cols = df.columns //--<-- Used to display the columns of the dataframe

    //used to replace Special char
    val cols = df.columns.map(x=>x.replaceAll(reg,""))

    //To put date in specific format use Format option
    val ndf = df.toDF(cols:_*).withColumn("DateofBirth",to_date($"DateofBirth","MM/dd/yyyy"))
      .withColumn("DateofJoining",to_date($"DateofJoining","MM/dd/yyyy"))

    val res = ndf.select(concat_ws("",$"FirstName",$"LastName").alias("FullName"),$"DateofBirth",$"DateofJoining")
      .withColumn("AgeofJoining",datediff($"DateofJoining",$"DateofBirth")/365).orderBy($"AgeofJoining".asc)
      .withColumn("Today",current_date())
      .withColumn("Currts",current_timestamp())
      .withColumn("DayOfWeek",date_format($"DateofJoining","EEEE"))
      .withColumn("DayOfWeek1",date_format($"DateofJoining","E"))
      .withColumn("NextDate" ,add_months($"today",6))
      .withColumn("BeforeDate" ,add_months($"today",-6))
      .withColumn("After8Days", date_add($"DateofJoining",8))
      .withColumn("Before8Days", date_add($"DateofJoining",-8))
      .withColumn("NextSunday",next_day($"today","Sunday"))
      .withColumn("lastDayofMonth",last_day($"today"))
      .withColumn("monthlastfriday",next_day(date_add(last_day($"today"),-7),"Fri"))
      .withColumn("monthsbtw",months_between($"DateofJoining",$"DateofBirth"))
      .withColumn("TruncM",trunc($"today","month")) // to get first day of month
      .withColumn("TruncY",trunc($"today","year"))
      //.withColumn("TruncAny",date_trunc(current_timestamp()))
      .withColumn("Qtr",quarter($"DateofJoining"))
      //.withColumn("dayofweek",dayofweek(current_date())) //Day of the week Sun - 1, Monday-2 ..
      .withColumn("dayofmonth",dayofmonth(current_date())) //how many days completed in the month
      .withColumn("dayofyear",dayofyear(current_date())) //from jan 1 how many days completed
      .withColumn("weekofyear",weekofyear(current_date()))
      .withColumn("Uxts",unix_timestamp(current_date())) //Shows seconds completed since 1 Jan 1970
      //.withColumn("Convertx", from_unixtime(to_date($"Uxts"))) //Convert back the Unix timestamp to date

    //datediff --> return no. of days between 2 dates.
    res.show(5,false)

    //---------------------------------------------------
    spark.stop()
  }
}