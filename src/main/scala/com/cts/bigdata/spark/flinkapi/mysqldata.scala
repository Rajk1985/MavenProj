package com.cts.bigdata.spark.flinkapi

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.types.Row

object mysqldata {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    val rowcol=new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)


    val jdbcformat = JDBCInputFormat.buildJDBCInputFormat()

      .setDrivername("oracle.jdbc.OracleDriver")
      //      url
      .setDBUrl("jdbc:oracle:thin://localhost:1521/orcl")
      .setUsername("scott")
      .setPassword("tiger")
      .setQuery("select  ename, job from emp")
      .setRowTypeInfo(rowcol)
      .finish()

    val ds = env.createInput(jdbcformat)
    ds.print()

  }
}
