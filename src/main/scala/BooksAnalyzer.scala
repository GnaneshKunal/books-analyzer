import org.apache.log4j._
import org.apache.spark._

object BooksAnalyzer extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Books Analyzer")

  var parallelize = sc.parallelize("Hello World".toList)

  parallelize.collect.foreach(print)

}
