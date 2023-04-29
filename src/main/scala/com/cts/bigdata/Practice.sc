import org.apache.spark.sql._
import org.apache.spark.sql.functions._

val spark =SparkSession.builder.master("local[*]").getOrCreate()
val sc = spark.sparkContext

//--Creating parallize RDD
val datarray = Array(1,2,4,5,6,7)
val rdd = sc.parallelize (datarray)
rdd.collect().foreach(println)

//-------Creating Hadoop Dataset----------------
val data = "F:\\bigdata\\Dataset\\bank-full.csv"
val rdd1 = sc.textFile(data)
//Transformation
val res = rdd1.map(x=>x.split(";"))
         .map(x=>(x(0),x(1),x(2),x(3),x(4)))
//Result Display
res.take(5).foreach(println)

//--- Creating dataFrame--------
val df = spark.read.format("csv").option("header","True").option("delimiter",";").load(data)

df.show(5)

df.createOrReplaceTempView("tab")
val res = spark.sql("Select * from tab")
res.show(10)


val res1 = df.withColumn("Umar", col("Age"))
res1.show(6)
spark.stop()
//-----------------------------------------
