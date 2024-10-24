import org.apache.spark.SparkContext
import scala.io
object test17 {
  def main(args:Array[String]): Unit = {
    println("hello how are you!")
    val sc = new SparkContext("local[*]","testapp")
    val input = sc.textFile("E:/Hadoop/Data/k1.txt")
    val rdd1 = input.flatMap(x => x.split(" "))
    val rdd2 = rdd1.map(x => (x, 1))
    val rdd3 = rdd2.reduceByKey((x, y) => x + y)
    val rdd4 = rdd3.sortBy(x=>x._2,false)
    //rdd3.collect().foreach(println)

    rdd3.take(2).foreach(println)



    scala.io.StdIn.readLine()



  }
}
