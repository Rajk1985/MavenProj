package com.cts.bigdata.spark.sparkstreaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
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


object kafkaProducer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("kafkaProducer").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //----------Write Logic Here--------------------------
    val logs = "C:\\Users\\RAJ\\Downloads\\access.log"
    // Reading data from log path
    val data = sc.textFile(logs)

    val topic = "logs"

    data.foreachPartition(rdd => {
      val props = new java.util.Properties()
      //  props.put("metadata.broker.list", "localhost:9092")
      //      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:9092")

      val producer = new KafkaProducer[String, String](props)

      rdd.foreach(x => {
        println(x)
        //sending to kafka broker
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString))

        Thread.sleep(5000)
      })
    })
  }
}