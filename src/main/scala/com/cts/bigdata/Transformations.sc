import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


val spark= SparkSession.builder.master("local[*]").getOrCreate()

val sc = spark.sparkContext
//This import is needed to invoke Spark keywords like show()
import spark.implicits._

//Example of reduceByKey
val data = "F:\\bigdata\\Dataset\\intro.txt"
//create a RDD
val rdd = sc.textFile(data)
rdd.foreach(println)

val pair = rdd.flatMap(x=> x.split(" "))
  .map(x=>(x ,1))
//val cnt = pair.reduceByKey((a,b)=> a+b)
val cnt = pair.reduceByKey(_+_)
cnt.foreach(println)

val columns = Seq("Seqno","Quote")
val data = Seq(("1", "Be the change that you wish to see in the world"),
  ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
  ("3", "The purpose of our lives is to be happy."))
val df = data.toDF(columns:_*)
//Example of cache
val dfCache = df.cache()
dfCache.show(false)

//-----------

val data = "F:\\bigdata\\Dataset\\intro.txt"
println("------Creating  RDD --------")
val rdd = sc.textFile(data)
rdd.foreach(println)

val word = rdd.toDF("text").filter(col("text").like("%Raj%"))
word.count()

val rdd1 = sc.parallelize(Array("jan","feb","mar","april","may","jun"),3)
val result = rdd1.coalesce(2)
result.foreach(println)

spark.stop()