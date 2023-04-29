package com.cts.bigdata.spark.sparkstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//Import this while doing streaming Also add sparkstreaming.jar in dependencies
import org.apache.spark.streaming._

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object nifiKakfaConsumerStream {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.streaming.kafka.allowNonConsecutiveOffsets","true").appName("nifiKakfaStream").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
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
    //Kafka Topic "nifi" Created  for Producer to stream data to consumer
    val topics = Array("nifi")

    //create dstream ...get data from kafka servers and prepare dstream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, //streamng context created earlier
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //dstream using Kakfa
    val lines = stream.map(record =>  record.value)
    lines.print()

      val res = lines.foreachRDD { a =>
        // Get the singleton instance of SparkSession
        val spark = SparkSession.builder.config(a.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        // spark streaming by default generate rdd.... rdd convert to datarame
        val df = spark.read.json(a) //read Json input
        df.show(false)

        //-------Writing to Oracle table-----
        // val df = spark.read.json(x).withColumn("newcol",explode($"results")).drop($"results").select($"nationality",$"seed",$"newcol.user.",$"newcol.user.location.",$"newcol.user.name.*").drop("location","name","picture")
        //  df.write.mode(SaveMode.Append).jdbc(ourl,"nifitab",oprop)
      }
      //---------------------------------------------------
    //Start Streaming
    ssc.start()
    ssc.awaitTermination() //Dont terminate session
  }
}