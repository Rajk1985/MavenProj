package com.cts.bigdata.spark.flinkapi

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object csvdata {
  case class aslcc (name:String, age:Int , city:String)

  def main(args: Array[String]) {

    // environment configuration for batch processing
    val env = ExecutionEnvironment.getExecutionEnvironment
//val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val data = "F:\\bigdata\\Dataset\\asl.csv"

    val ds = env.readCsvFile[aslcc](data, ignoreFirstLine = true)
        ds.print()
    //dataset convert to table api
    val tab = tEnv.fromDataSet(ds)

    // dataset register as table called asl
    tEnv.registerTable("asl",tab)

    val res = tEnv.sqlQuery("Select * from asl where age > 20")

    //convert table api to dataset
    val result = tEnv.toDataSet[aslcc](res)

    result.print()

   // val output = "F:\\bigdata\\flinkcsv.csv"

   // result.writeAsCsv(output).setParallelism(1)

    //env.execute()

  }
}
