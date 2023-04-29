val text = "Select count(*) as cnt   from NGRAW"

val splitText = text.split("\\W+")

println(splitText(3))

val delim = text.drop(text.indexOfSlice("from ")+5).takeWhile(_.isLetter)
println(delim)

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().master("local[*]").getOrCreate()
val sc = spark.sparkContext
val LoginActivityTypeId = 0
val LogoutActivityTypeId = 1

val readUserData =
      Array(
        (1, "Elmon, Patrick"),
        (2, "Mathew, John"),
        (3, "X, Mr."))

val rdd1 = sc.parallelize(readUserData)
val df1 = spark.createDataFrame(rdd1).toDF("id","Name")

val readUserActivityData =
      Array(
        (1, LoginActivityTypeId, 1514764800000L),
        (2, LoginActivityTypeId, 1514808000000L),
        (1, LogoutActivityTypeId,1514829600000L),
        (1, LoginActivityTypeId, 1514894400000L))

val rdd2 = sc.parallelize(readUserActivityData)
val df2 = spark.createDataFrame(rdd2).toDF("id","Activity","TimeStamp")

//df1.show()
//df2.show()

df1.createOrReplaceTempView("tab1")
df2.createOrReplaceTempView("tab2")

val res = spark.sql("SELECT a.Name , max(b.TimeStamp) from tab1 a join tab2 b on a.id =b.id group by a.name")

res.show()
spark.stop()
//------------

/*case class User(userId: Long, userName: String)
case class UserActivity(userId: Long, activityTypeId: Int, timestampEpochMs: Long)

val LoginActivityTypeId = 0
val LogoutActivityTypeId = 1

private def readUserData(sparkSession: SparkSession): DataFrame = {
  sparkSession.createDataFrame(
    sparkSession.sparkContext.parallelize(
      Array(
        User(1, "Elmon, Patrick"),
        User(2, "Mathew, John"),
        User(3, "X, Mr."))
    )
  )
}
private def readUserActivityData(sparkSession: SparkSession): DataFrame = {
  sparkSession.createDataFrame(
    sparkSession.sparkContext.parallelize(
      Array(
        UserActivity(1, LoginActivityTypeId, 1514764800000L),
        UserActivity(2, LoginActivityTypeId, 1514808000000L),
        UserActivity(1, LogoutActivityTypeId, 1514829600000L),
        UserActivity(1, LoginActivityTypeId, 1514894400000L))
    )
  )
}

//def calculate(sparkSession: SparkSession): Unit = {
  val UserTableName = "user"
  val UserActivityTableName = "userActivity"

  val userDf = readUserData(SparkSession)
  val userActivityDf = readUserActivityData(SparkSession)

  userDf.createOrReplaceTempView(UserTableName)
  userActivityDf.createOrReplaceTempView(UserActivityTableName)

  val result = spark.sql(s"SELECT b.UserName , max(a.TimeStamp) from UserActivityTableName a join UserTableName b on a.user_id =b.user_id group by b.username")

  result.show()

spark.stop()*/

