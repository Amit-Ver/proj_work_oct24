import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_date, datediff, initcap, lit, sum, to_date, when}
import org.apache.spark.sql.functions._

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

//    val perfdf = sales.select(initcap(col("name")).alias("name"),
//      col("total_sales"),
//      when(col("total_sales")>50000,"Excellent")
//        .when(col("total_sales") between(25000,50000), "Good")
//        .otherwise("Need Improvement").alias("performanceStatus")
//    )
//    perfdf.show()

//    3) Project allocation and workload analysis
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

//    val wldf = workload.select( initcap(col("name")).alias("name"),col("project")
//      ,col("hours"), when(col("hours")>200,"Overloaded")
//                    .when(col("hours") between(100,200),"Balanced")
//                    .otherwise("Underutilized").alias("category")
//    )
////    aggregated workload status count by category
//    val finaldf = wldf.groupBy(col("category")).count()
//    wldf.show()
//    finaldf.show()

//    4)
    val employeesot = List(
      ("karthik", 62),
      ("neha", 50),
      ("priya", 30),
      ("mohan", 65),
      ("ajay", 40),
      ("vijay", 47),
      ("veer", 55),
      ("aatish", 30),
      ("animesh", 75),
      ("nishad", 60)
    ).toDF("name", "hours_worked")

//    val otdf = employeesot.select(initcap(col("name")).alias("name"), col("hours_worked")
//      ,when(col("hours_worked") > 60, "Excessive Overtime")
//        .when(col("hours_worked") between(45, 60), "Standard Overtime")
//        .otherwise("No Overtime").alias("OvertimeStatus")
//    ).groupBy(col("OvertimeStatus")).count()
////      val otcntdf = otdf.groupBy(col("OvertimeStatus")).count()
//    otdf.show()
//    otcntdf.show()

//    5) Customer age grouping
        val customers = List(
            ("karthik", 22),
            ("neha", 28),
            ("priya", 40),
            ("mohan", 55),
            ("ajay", 32),
            ("vijay", 18),
            ("veer", 47),
            ("aatish", 38),
            ("animesh", 60),
            ("nishad", 25)
        ).toDF("name", "age")

//    val custdf = customers.select(initcap(col("name")).alias("name"), col("age")
//      , when(col("age") < 25, "Youth")
//        .when(col("age") between(25, 45), "Adult")
//        .otherwise("Senior").alias("AgeStatus")
//    )
//    val custcntdf = custdf.groupBy(col("AgeStatus")).count()
//    custcntdf.show()
//    custdf.show()

//    6) Vehicle Mileage
    val vehicles = List(
      ("CarA", 30),
      ("CarB", 22),
      ("CarC", 18),
      ("CarD", 15),
      ("CarE", 10),
      ("CarF", 28),
      ("CarG", 12),
      ("CarH", 35),
      ("CarI", 25),
      ("CarJ", 16)
    ).toDF("vehicle_name", "mileage")

//    val vehdf = vehicles.select(col("vehicle_name"), col("mileage")
//      , when(col("mileage") > 25, "High Efficiency")
//        .when(col("mileage") between(15, 25), "Moderate Efficiency")
//        .otherwise("Low Efficiency").alias("MileageStatus")
//    )

//    7) student score
    val students = List(
      ("karthik", 95),
      ("neha", 82),
      ("priya", 74),
      ("mohan", 91),
      ("ajay", 67),
      ("vijay", 80),
      ("veer", 85),
      ("aatish", 72),
      ("animesh", 90),
      ("nishad", 60)
    ).toDF("name", "score")

//    val stdf = students.select(col("name"),col("score"),
//      when(col("score")>=90,"Excellent")
//        .when(col("score") between(75,89),"Good")
//        .otherwise("Needs Improvement").alias("Grade")
//    )
//    val stcntdf = stdf.groupBy(col("Grade")).count()
//    stdf.show()
//    stcntdf.show()

//    8) product inventory check
      val inventory = List(
        ("ProductA", 120),
        ("ProductB", 95),
        ("ProductC", 45),
        ("ProductD", 200),
        ("ProductE", 75),
        ("ProductF", 30),
        ("ProductG", 85),
        ("ProductH", 100),
        ("ProductI", 60),
        ("ProductJ", 20)
      ).toDF("product_name", "stock_quantity")

    val invdf = inventory.select(col("product_name"), col("stock_quantity"),
      when(col("stock_quantity") > 100, "OverStocked")
        .when(col("stock_quantity") between(50, 100), "Normal")
        .otherwise("Low Stock").alias("StockStatus")
    )
    val invcntdf = invdf.groupBy(col("StockStatus")).count()
    invcntdf.show()
    invdf.show()

//    10) employee bonus calculation based on performance and dept.
    val empdept = List(
      ("karthik", "Sales", 85),
      ("neha", "Marketing", 78),
      ("priya", "IT", 90),
      ("mohan", "Finance", 65),
      ("ajay", "Sales", 55),
      ("vijay", "Marketing", 82),
      ("veer", "HR", 72),
      ("aatish", "Sales", 88),
      ("animesh", "Finance", 95),
      ("nishad", "IT", 60)
    ).toDF("name", "department", "performance_score")


  }
}
