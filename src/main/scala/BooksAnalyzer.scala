import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession

import scala.io.Codec

object BooksAnalyzer extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder.appName("Books Analyzer").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  var parallelize = sc.parallelize("Hello World".toList)

  parallelize.collect.foreach(print)

}
