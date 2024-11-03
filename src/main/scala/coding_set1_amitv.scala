import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, datediff, initcap, lit, to_date, when}

object coding_set1_amitv {
  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder().appName("codingset1").master("local[*]").getOrCreate()
    import spark.implicits._

    // 1)   finding the status of active employees based on lastcheckin within 7 days.
    val employees = List(
      ("karthik", "2024-11-01"),
      ("neha", "2024-10-20"),
      ("priya", "2024-10-28"),
      ("mohan", "2024-11-02"),
      ("ajay", "2024-09-15"),
      ("vijay", "2024-10-30"),
      ("veer", "2024-10-25"),
      ("aatish", "2024-10-10"),
      ("animesh", "2024-10-15"),
      ("nishad", "2024-11-01"),
      ("varun", "2024-10-05"),
      ("aadil", "2024-09-30")
    ).toDF("name", "last_checkin")

//    val activedf = employees.select(initcap(col("name")).alias("name")
//      , to_date(col("last_checkin")).alias("lastcheckin")
//      , lit(current_date()).alias("todaydate")
//      //      ,datediff(col("todaydate"),col("lastcheckin")).alias("datedifference")
//      //                                ,when(col("last_checkin")< (current_date()-7),"Inactive")
//      //                                .otherwise("Active")
//    )
//    val finaldf = activedf.withColumn("daysdiff", datediff(col("todaydate"), col("lastcheckin")))
//      .withColumn("Status", when(col("daysdiff") < 7, "Active").otherwise("InActive"))
//      .drop(col("todaydate")).drop(col("daysdiff"))
//    activedf.show()
//    finaldf.show()
//    //    activedf.printSchema()
//    //    finaldf.printSchema()

    //   2) Sales Performance based on total sales.

    val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)
    ).toDF("name", "total_sales")

    val perfdf = sales.select(initcap(col("name")).alias("name"),
      col("total_sales"),
      when(col("total_sales")>50000,"Excellent")
        .when(col("total_sales") between(25000,50000), "Good")
        .otherwise("Need Improvement").alias("performanceStatus")
    )
    perfdf.show()

    val workload = List(
      ("karthik", "ProjectA", 120),
      ("karthik", "ProjectB", 100),
    ("neha", "ProjectC", 80),
    ("neha", "ProjectD", 30),
    ("priya", "ProjectE", 110),
    ("mohan", "ProjectF", 40),
    ("ajay", "ProjectG", 70),
    ("vijay", "ProjectH", 150),
    ("veer", "ProjectI", 190),
    ("aatish", "ProjectJ", 60),
    ("animesh", "ProjectK", 95),
    ("nishad", "ProjectL", 210),
    ("varun", "ProjectM", 50),
    ("aadil", "ProjectN", 90)
    ).toDF("name", "project", "hours")



  }
}
