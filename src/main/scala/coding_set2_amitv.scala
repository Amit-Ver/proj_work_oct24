import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, currentRow, lag, lead, least, unboundedPreceding}
import org.apache.spark.sql.types.IntegerType
object coding_set2_amitv {
  def main(args:Array[String]): Unit = {
    val sc = new SparkContext("local[*]","testapp")
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    sc.setLogLevel("Error")
    import spark.implicits._
    //    val salesdata = List(
    //      (1, "KitKat", 1000, "2021-01-01"),
    //      (1, "KitKat", 2000, "2021-01-02"),
    //      (1, "KitKat", 1000, "2021-01-03"),
    //      (1, "KitKat", 2000, "2021-01-04"),
    //      (1, "KitKat", 3000, "2021-01-05"),
    //      (1, "KitKat", 1000, "2021-01-06")
    //    ).toDF("ProdID", "Prodname","Price","ProdManfdt")
    //
    //
    //    val winspec = Window.orderBy(col("ProdManfdt"))
    //    val prevdf = salesdata.withColumn("pre_prc", lag(col("Price"),1).over(winspec) )
    //    prevdf.show()
    //    val pricediff = prevdf.withColumn("price_diff",col("Price") - col("pre_prc"))
    //    pricediff.show()

    val datadf = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("E:/Hadoop/Data/emp_sal.txt")
    datadf.show()
//    val distdf = datadf.distinct()
//    distdf.show()
//    val newdf = datadf.dropDuplicates("name")
//    newdf.show()

    val windf = Window.orderBy(col("id").desc)
//      .rowsBetween(Window.unboundedPreceding,Window.currentRow)
//    val leaddf = newdf.withColumn("leadprice", lead(col("salary"),2).over(windf))
//      .withColumn("lagprice", lag(col("salary"), 3).over(windf) )
//      .withColumn("percent", (col("salary")-col("lagprice"))/col("salary")*100)

    val lagdf = datadf.withColumn("prev_sal",lag(col("salary"),1).over(windf))
      .withColumn("price_diff",col("salary")-col("prev_sal"))
//
    lagdf.show()

    val filterdf = lagdf.filter(col("price_diff")> 1500)
    filterdf.show()

    val countdf = datadf.count()

  }

}
