package com.cts.bigdata.spark.sparkstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//Import this while doing streaming Also add sparkstreaming.jar in dependencies
import org.apache.spark.streaming._

//import java.util.Properties

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils


object TwitterDataStream {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("TwitterDataStream").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    // Below are Venu Twitter keys
    //val APIkey= "Xe8OaomGxpyimZdUDiHVBubXB"
    //val APIsecretkey= "S3Oczd5emhVqXiufeP1JsJmmk3LeYyItfaR0sFrodu7HwwYEsW"
    //val Accesstoken = "181460431-ecrA8u8mY9qujNB7pbiLp0GwrWr6qNe3DRSOEYfS"
    //val Accesstokensecret = "2yK5r7mCYS4J4DBNXus7FWzYOOCR6pVXVvpLhG1M2r3aT"

    //My twitter Keys
    val APIkey= "aPgcsBBKXTo26yCigO4TWeLlJ"
    val APIsecretkey= "V5lMplMsVLOzuNdUMlplUJJ4F8ECCwnrpfe3mjn1280UCY3gek"
    val Accesstoken = "145335419-4VI0DCrDYo8pd4P3lj93fNF8kJt76vczwgmqfvbI"
    val Accesstokensecret = "Us6y2FyovciWiXlapL0lp4Z6SQ7BfeixZ9RcUQzOBvgZ3"

    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)
    //val lines = ssc.socketTextStream("localhost", 9999)

    val searchFilter = "Corona, Bengal Election,india"//"trump,CORONA, USA election, Election2020, DONALD TRUMP "
    // create dstream
    val tweetStream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString))


    tweetStream.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = x.map(x => (x.getText(), x.getUser().getScreenName(),x.getCreatedAt().getTime())).toDF("msg", "username","createdDate")

      df.printSchema()
      df.show(false)

      //Create Temp Table for sql processing
      df.createOrReplaceTempView("tab")
      //val res = spark.sql("select * from tab where msg like '%https://%'")
      val res = spark.sql("select * from tab where username like 'Raj%'")
      res.show()

      //Writing the data to local
      val path = "F:\\bigdata\\Dataset\\output\\twitterdata"
      res.write.mode(SaveMode.Append).format("csv").option("header","true").save(path)

      //Saving the information in Database on AWS
      /*    val ourl ="jdbc:oracle:thin:@//sqooppoc.cjxashekxznm.ap-south-1.rds.amazonaws.com:1521/ORCL"
            val oprop = new java.util.Properties()
            oprop.setProperty("user","ousername")
            oprop.setProperty("password","opassword")
            oprop.setProperty("driver","oracle.jdbc.OracleDriver")
            res.write.mode(SaveMode.Append).jdbc(ourl,"tweets",oprop)*/
    }
    //---------------------------------------------------
    //Start Streaming
    ssc.start()
    ssc.awaitTermination() //Dont terminate session
  }
}
