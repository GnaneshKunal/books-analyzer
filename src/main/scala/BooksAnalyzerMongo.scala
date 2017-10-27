//import statements
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark._
import com.mongodb._
import com.mongodb.spark.MongoSpark

object BooksAnalyzerMongo extends App {
 //set log level to error
  Logger.getLogger("org").setLevel(Level.ERROR)//prints error logs only
  // initialization
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Books Analyzer Mongo")
    .config("spark.mongodb.input.uri", "mongodb://monster:monster@127.0.0.1:27017/book-miner.bookreviews")
    .config("spark.mongodb.output.uri", "mongodb://monster:monster@127.0.0.1:27017/book-miner.bookreviews")
    .getOrCreate()
  
  val sc = spark.sparkContext
  //load dataset from mongo db as a rdd
  val rdd = MongoSpark.load(sc).persist()
  // print 
  println {
    "TOP 10 AUTHORS"
  }
 
  val top10Authors =
    rdd.map(x => (x.getString("author"), 1)) //use map fn to get author and create tuple(author,no of book=1)
    .reduceByKey(_ + _) //create condensed tuple (author,total no of books) where key is author 
    .sortBy(_._2, ascending = false) //descending sort by number of books
    .take(10) // take top 10 tuples alone 

  top10Authors.foreach(println) //print the top 10 authors

  val harryPotterBooks = rdd.filter(_.get("title").toString.contains("Harry Potter")) //filter the rdd to get rows where book titles contain harry potter 
  //print the count of harrypotter books
  println {
    "HARRY POTTER BOOKS COUNT: " + harryPotterBooks.count()
  }
  
  println {
    "BOOKS WITH MOST EDITIONS"
  }

  val booksWithMostEditions =
    rdd.map(x => (x.get("title"), 1)) // use map fn to get title and create tuple(title,Edition count=1)
      .reduceByKey(_ + _) //create condensed tuple (title,edition count) where key is title
      .sortBy(_._2, ascending = false) //descending sort edition count

  booksWithMostEditions.take(5).foreach(println) //print top 5 books with most number of editions

  println {
    "BOOKS WITH 5 STAR REVIEWER RATINGS"
  }

  val booksWithFiveStarReviewerRatings = rdd.filter(_.get("reviewerRatings") == 5) // filter rdd to get rows where reviewer ratings is 5 

  booksWithFiveStarReviewerRatings.take(5).foreach(x => println(x.get("title"))) //print first 5 book titles with 5 star rating

  println{
    "BOOKS WITH 1 STAR REVIEWER RATINGS"
  }

  val booksWith1StarRatings = rdd
    .filter(_.get("reviewerRatings") == 1) //filter rdd to get rows where reviewer ratings is 1

  booksWith1StarRatings
    .take(5) foreach(x => println { 
    x.get("title")                //print first 5 book titles with 1 star rating
  })

  println {
    "TITLES STARTING WITH 'A'"
  }

  val booksStartsWithA = rdd
    .filter(_.get("title").toString.charAt(0) == 'A') //filter rdd to get rows where book title start with a

  booksStartsWithA take 5 foreach(x => println { //print first 5 books whose name start with "A"
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

  print {
    "AVERAGE RATINGS: "
  }

  val averageRatings = booksWithNoRatings
    .map(x => (1, x.get("reviewerRatings").toString.toInt))
    .reduceByKey(_ + _).first._2.toDouble / booksWithNoRatings.count()

  println {
    averageRatings
  }

  println {
    "BOOKS WITH LARGEST TITLE"
  }

  val booksWithLargestTitle = rdd
    .map(x => (
      x.get("title"),
      x.get("title")
        .toString.length
    ))
    .sortBy(_._2, ascending = false)

  spark.stop()
}
