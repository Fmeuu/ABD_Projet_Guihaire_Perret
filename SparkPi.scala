import org.apache.spark._

import scala.math.random

object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2



val tab = Array(10,100,1000,10000)

for(n <- tab){
val a = 1
val b = 10
val p = 1.0*(b-a)/n

val t0 = System.nanoTime()
    val count = spark.parallelize(1 until n, slices).map { i =>
        
	1.0/(a + i*p)

      }.reduce(_ + _)

    val t1 = System.nanoTime()

println("for size : " + n)
    println("Elapsed time: " + (t1 - t0) + "ns")

    println("result = " + count*p)

}
    spark.stop()
  }
}
