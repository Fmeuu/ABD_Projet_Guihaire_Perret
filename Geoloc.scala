
import scala.math.random
import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.rdd.RDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession


object Geoloc {
	def main(args: Array[String]) {
		val spark = SparkSession
		.builder
		.appName("Geoloc")
		.getOrCreate()
		
		
                var textFile = spark.sparkContext.textFile("./src/main/scala/onlyBay.txt")
		textFile = textFile.map(line => line.replaceFirst("[A-Za-z0-9]* ", ""))
		val lineSplitted = textFile.flatMap(line => line.split(" "))
  		val byPaire = lineSplitted.map(line => line.split(","))
		//Quesion 1
		//.map(line => (line(0), line(1).toInt))
		//Question 2
		//.map(line => (line(0), 1))
		.map(line => (line(0), (Array(line(1).toFloat))))
		//Question 1 + 2
		//.reduceByKey(_ + _).sortBy(- _._2)
		.reduceByKey(_ ++ _)
		.mapValues(a => a.sum/a.size)
		byPaire.saveAsTextFile("./result.txt")

		spark.stop()
	}
}
