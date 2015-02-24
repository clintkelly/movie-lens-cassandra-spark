import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.joda.time
import org.joda.time.DateTime

case class ReviewWithMetadata(userId: Long, reviewTime: DateTime, movieId: Long, movieTitle: String, movieUrl: String, rating: Int)

/**
 * Import movie ratings and perform some denormalization.
 */
object ImportRatings {
  import com.datastax.spark.connector._

  val genreList = Seq("unknown", "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime", "Documentary",
  "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western")

  def writeDenormalizedRatings(userRatingsLines: RDD[String], movieMetadataLines: RDD[String]): Unit = {

    // Keys for both will be the movie IDs so that we can get the movie metadata.
    val userRatingsByMovieId = userRatingsLines.map { line: String =>
      val tokens: Array[String] = line.split("\\s+")
      require(tokens.size == 4)
      (tokens(1), tokens)
    }

    userRatingsByMovieId.take(10).foreach(println)

    val movieMetadataByMovieId = movieMetadataLines.map { line: String =>
      val tokens: Array[String] = line.split("\\|")
      require(tokens.size == 24, s"Got ${tokens.size} tokens for line $line")
      (tokens(0), tokens)
    }
    movieMetadataByMovieId.take(10).foreach(println)

    val userRatingsAndMovieMetadataByMovieId = userRatingsByMovieId.join(movieMetadataByMovieId)

    // Manipulate this into user ratings again, but with movie metadata.
    val userRatingsWithMetadata = userRatingsAndMovieMetadataByMovieId.map {
      case (movieId: String, (userRatingInfo: Array[String], movieMetadata: Array[String])) =>
        ReviewWithMetadata(
          userId = userRatingInfo(0).toLong,
          rating = userRatingInfo(2).toInt,
          reviewTime = new DateTime(userRatingInfo(3).toLong * 1000),
          movieTitle = movieMetadata(1),
          movieUrl = movieMetadata(4),
          movieId = movieId.toLong
        )
    }

    val firstFew = userRatingsWithMetadata.take(10)
    firstFew.foreach(println)
    userRatingsWithMetadata.saveToCassandra("movies", "reviews")
  }

  def writeUserMetadata(userMetadataLines: RDD[String]): Unit = {
    // Pull out the fields and write to Cassandra as a user-defined type.
    userMetadataLines.map { line =>
      val tokens = line.split("\\|")
      require(tokens.size == 5, s"Problem with $line")

      val userId: Long = tokens(0).toLong

      val userInfo = UDTValue.fromMap(Map(
        "age" -> tokens(1).toInt,
        "gender" -> tokens(2),
        "occupation" -> tokens(3),
        "zipcode" -> tokens(4)))
      (userId, userInfo)
    }.saveToCassandra("movies", "user_metadata", SomeColumns("user_id", "info"))
  }

  def writeMovieMetadata(movieMetadataLines: RDD[String]): Unit = {
    movieMetadataLines.map { line =>
      val tokens = line.split("\\|")
      require(tokens.size == 24, s"Problem with $line")

      val movieId: Long = tokens(0).toLong
      val title: String = tokens(1)
      //val theaterDate = tokens(2)
      //val videoDate = tokens(3)
      val url: String = tokens(4)

      // Scala poetry!
      val genres = (genreList, tokens.takeRight(19)).zipped.flatMap { case (genreName: String, zeroOrOne: String) =>
          require(zeroOrOne == "0" || zeroOrOne == "1")
          zeroOrOne match {
            case "0" => None
            case "1" => Some(genreName)
          }
      }

      (movieId, title, url, genres)

    }.saveToCassandra("movies", "movie_metadata", SomeColumns("movie_id", "title", "url", "genres"))
  }


  def main(args: Array[String]): Unit = {

    // Set up SparkContext.
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.output.consistency.level", "LOCAL_ONE")

    val sc = new SparkContext(conf)

    // Read user ratings.
    // user_id movie_id rating timestamp
    val userRatingsLines = sc.textFile("ml-100k/u.data")
    userRatingsLines.cache()

    // Read the movie metadata.
    // movie_id movie_title release_date video_date url genres...
    val movieMetadataLines = sc.textFile("ml-100k/u.item")
    movieMetadataLines.cache()

    // Read the user metadata.
    val userMetadataLines = sc.textFile("ml-100k/u.user")
    userMetadataLines.cache()

    writeDenormalizedRatings(userRatingsLines, movieMetadataLines)
    writeUserMetadata(userMetadataLines)
    writeMovieMetadata(movieMetadataLines)

  }

}
