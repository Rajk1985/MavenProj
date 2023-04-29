/******************************
 * Objective of this program to show usage of a method written in one package
 * being called from another package
 ******************************/
package com.cts.bigdata.SelfLearningScala

//Importing other package to use functions/ methods in current package
import com.cts.bigdata.spark.sparksql._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ObjectCalling {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ObjectCalling").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //----------Write Logic Here--------------------------
    val dff = ReadCsv.readdata   //Objectname.MethodName to call the method
    //val dff = com.cts.bigdata.spark.sparksql.ReadCsv.readdata .This syntax can also be used
    dff.show(2)

    //Create temp view for analytics
    dff.createOrReplaceTempView("CSVDATA")
    val res = spark.sql("Select * from CSVDATA where State = 'OH'")
    res.show()

    //---------------------------------------------------
    spark.stop()

  }
}