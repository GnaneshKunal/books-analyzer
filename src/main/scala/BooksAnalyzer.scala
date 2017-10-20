import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession

import scala.io.Codec

object BooksAnalyzer extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder.appName("Books Analyzer").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  var parallelize = sc.parallelize("Hello World".toList)

  val bookReviews = spark.read.csv("./src/main/resources/reviews.csv")

  println {
    bookReviews.collect.filter(x => x(0).toString.indexOf("(Harry Potter, #") >= 0).distinct.foreach(x => println(x(0) + "\t" + x(3)))
  }


  parallelize.collect.foreach(print)

  spark.stop()

}
