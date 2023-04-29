package com.cts.bigdata.spark.sparkstreaming

import org.apache.spark.sql._

//Import this while doing streaming Also add sparkstreaming.jar in dependencies
import org.apache.spark.streaming._

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object kafkaConsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkstreaming").getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10)) // Specify the time to feed Live data

    // set the Log to display only error
    spark.sparkContext.setLogLevel("ERROR")

    //----------Write Logic Here--------------------------
    //Setting Kafka Properties
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",//--<Here you can mention multiple servers
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //Kafka Topic "rktopic" Created  for Producer to stream data to consumer
    val topics = Array("rktopic")

    //create dstream ...get data from kafka servers and prepare dstream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, //streamng context created earlier
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //dstream using Kakfa
    val lines = stream.map(record =>  record.value)
    lines.print() // Print the line received from Producer

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
      val info = spark.sql("select * from tab")

        //Creating Local Database Oracle instance
        val url ="jdbc:oracle:thin://localhost:1521/orcl"
        val mprop = new java.util.Properties()
        mprop.setProperty("user","scott")
        mprop.setProperty("password","tiger")
        mprop.setProperty("driver","oracle.jdbc.OracleDriver")

        //Write to Oracle database as livedata table
        info.write.mode(SaveMode.Append).jdbc(url,"kafkaData",mprop)

    }
    //Start Streaming
    ssc.start()
    ssc.awaitTermination() //Dont terminate session
    //---------------------------------------------------

  }
}