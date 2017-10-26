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

  val rdd = MongoSpark.load(sc).persist()

  println {
    "TOP 10 AUTHORS"
  }

  val top10Authors =
    rdd.map(x => (x.getString("author"), 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .take(10)

  top10Authors.foreach(println)

  val harryPotterBooks = rdd.filter(_.get("title").toString.contains("Harry Potter"))

  println {
    "HARRY POTTER BOOKS COUNT: " + harryPotterBooks.count()
  }

  println {
    "BOOKS WITH MOST EDITIONS"
  }

  val booksWithMostEditions =
    rdd.map(x => (x.get("title"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

  booksWithMostEditions.take(5).foreach(println)

  println {
    "BOOKS WITH 5 STAR REVIEWER RATINGS"
  }

  val booksWithFiveStarReviewerRatings = rdd.filter(_.get("reviewerRatings") == 5)

  booksWithFiveStarReviewerRatings.take(5).foreach(x => println(x.get("title")))

  println{
    "BOOKS WITH 1 STAR REVIEWER RATINGS"
  }

  val booksWith1StarRatings = rdd
    .filter(_.get("reviewerRatings") == 1)

  booksWith1StarRatings
    .take(5) foreach(x => println {
    x.get("title")
  })

  println {
    "Books with 0 Star ReviewerRatings"
  }

  val booksWith0StarRatings = rdd
    .filter(_.get("reviewerRatings") == 0)

  booksWith0StarRatings
    .take(5) foreach(x => println {
    x.get("title")
  })

  println {
    "TITLES STARTING WITH 'A'"
  }

  val booksStartsWithA = rdd
    .filter(_.get("title").toString.charAt(0) == 'A')

  booksStartsWithA take 5 foreach(x => println {
    x.getString("title")
  })

  println {
    "BOOKS WITH BIGGEST REVIEW"
  }

  val booksWithBiggestReview = rdd
    .map(x => (
      x.get("title"),
      x.get("review")
        .toString
        .split("\\W+").length
    ))

  booksWithBiggestReview.distinct.sortBy(_._2, ascending = false)
    .take(10).foreach(println)


  println {
    "BOOKS WITH NO RATINGS"
  }

  val booksWithNoRatings = rdd
    .filter(_.get("reviewerRatings") != "")

  booksWithNoRatings.distinct.take(5)
    .foreach(x => println {
      x.get("title")
    })

  spark.stop()
}
