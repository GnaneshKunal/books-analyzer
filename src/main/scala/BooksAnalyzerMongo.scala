import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark._
import com.mongodb._
import com.mongodb.spark.MongoSpark

object BooksAnalyzerMongo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Books Analyzer Mongo")
    .config("spark.mongodb.input.uri", "mongodb://monster:monster@127.0.0.1:27017/book-miner.bookreviews")
    .config("spark.mongodb.output.uri", "mongodb://monster:monster@127.0.0.1:27017/book-miner.bookreviews")
    .getOrCreate()

  val sc = spark.sparkContext

  val rdd = MongoSpark.load(sc)

  val filteredRdd = rdd.filter(_.getInteger("bookID") < 10)
  println {
    filteredRdd.first.toJson
  }

  println {
    rdd.count
  }

  spark.stop()


}
