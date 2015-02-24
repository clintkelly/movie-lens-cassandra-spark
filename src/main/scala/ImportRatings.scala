import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.joda.time
import org.joda.time.DateTime

case class ReviewWithMetadata(userId: Long, reviewTime: DateTime, movieId: Long, movieTitle: String, movieUrl: String, rating: Int)

/**
 * Import movie ratings and perform some denormalization.
 */
object ImportRatings {

  def main(args: Array[String]): Unit = {

    // Set up SparkContext.
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext(conf)

    // Read user ratings.
    // user_id movie_id rating timestamp
    val userRatingsLines = sc.textFile("ml-100k/u.data")

    // Read the movie metadata.
    // movie_id movie_title release_date video_date url genres...
    val movieMetadataLines = sc.textFile("ml-100k/u.item")

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

    import com.datastax.spark.connector._

    userRatingsWithMetadata.saveToCassandra("movies", "reviews")

  }

}
