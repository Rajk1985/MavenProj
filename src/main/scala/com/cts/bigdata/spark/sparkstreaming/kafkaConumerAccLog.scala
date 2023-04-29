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

object kafkaConumerAccLog {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("kafkaConumerAccLog").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    //Setting Kafka Properties
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092", //--<Here you can mention multiple servers
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //Kafka Topic "logs" Created  for Producer to stream data to consumer
    val topics = Array("logs")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, //streamng context created earlier
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //dstream
    val lines = stream.map(record => record.value)
    lines.print()

    //Convert the streaming data to Dataframe for processing
    val res = lines.foreachRDD { a =>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(a.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      // spark streaming by default generate rdd.... rdd convert to datarame
      //Map columns names
      val df = a.map(x=>x.split(" ")).map(x=>(x(0),x(1),x(2))).toDF("ip","date","time")
      df.show()
    }
  }
}