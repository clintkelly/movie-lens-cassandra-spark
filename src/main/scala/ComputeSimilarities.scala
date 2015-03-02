/**
 * Use MLlib to run collaborative filtering.
 */

import org.apache.log4j.{Logger, Level}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

object ComputeSimilarities {
  import com.datastax.spark.connector._
  import org.apache.spark.SparkContext._

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // Set up SparkContext.
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.output.consistency.level", "LOCAL_ONE")

    val sc = new SparkContext(conf)

    // Get a map from movie IDs to movie titles.
    val movieIdsToTitles: Map[Long, String] = sc
      .cassandraTable("movies", "movie_metadata")
      .select("movie_id", "title")
      .map(row => (row.get[Long]("movie_id"), row.get[String]("title")))
      .collect()
      .toMap

    // Organize the ratings into a matrix where each row is a user and each column is a movie.
    val ratingsByUser: RDD[(Int, Iterable[(Int, Double)])] = sc
      .cassandraTable[(Long, Long, Int)]("movies", "reviews")
      .select("user_id", "movie_id", "rating")
      .map { case (userId: Long, movieId: Long, rating: Int) =>
        (userId.toInt, (movieId.toInt, rating.toDouble))
      }.groupByKey().cache()

    val rows = ratingsByUser.map { case (userId: Int, ratings: Iterable[(Int, Double)]) =>
        // For now I just hard coded a number slightly higher than the total number of movies in the database.
        Vectors.sparse(1700, ratings.toSeq)
    }

    val mat = new RowMatrix(rows)

    val itemSimilarities: RDD[(Long, (Long, Double))] = mat
      .columnSimilarities()
      // Need to transform this into item,item similarity pairs and then group by first item.
      // Need to duplicate (e.g., A,B similarity and B,A similarity) so that we can get lists of most-similar movies
      // for all movies.
      .toIndexedRowMatrix()
      .rows
      .flatMap { row: IndexedRow =>
        val movieA: Long = row.index
        val movieIdsAndSimilarities: Seq[(Long, (Long, Double))] = row
          .vector
          .toArray
          .zipWithIndex
          .filter { // select elements with similarity > 0.01 (chosen arbitrarily).
            _._1 > 0.01
          }
          .flatMap { case (similarity: Double, movieB: Int) =>
            Seq((movieA, (movieB.toLong, similarity)), (movieB.toLong, (movieA, similarity)))
          }
        movieIdsAndSimilarities
      }

      val moviesWithMostSimilar: RDD[(Long, Seq[(Long, String, Double)])] = itemSimilarities
        .groupByKey
        .map { case (movieId: Long, similarities: Iterable[(Long, Double)]) =>
          // Convert the similarities to a Seq and take the most similar ones, then add titles.
          val mostSimilarMovies: Seq[(Long, Double)] = similarities.toSeq.sortBy(_._2).reverse
          val mostSimilarMoviesWithTitles: Seq[(Long, String, Double)] = mostSimilarMovies
            .map { case (id: Long, similarity: Double) =>
              (id, movieIdsToTitles.getOrElse(id, "No title - WTF"), similarity)
            }
          (movieId, mostSimilarMoviesWithTitles.take(10))
        }

      moviesWithMostSimilar.collect().take(100).foreach { case (movieId: Long, recs: Seq[(Long, String, Double)]) =>
          val title = movieIdsToTitles.getOrElse(movieId, "WTF")
          println(title + ":" + recs)
      }

      // Convert to Cassandra UDT:
      val udtSimilarities: RDD[(Long, String, Set[UDTValue])] = moviesWithMostSimilar.map {
        case (movieId: Long, recs: Seq[(Long, String, Double)]) =>
          val movieSet: Set[UDTValue] = recs.map { case (id: Long, title: String, similarity: Double) =>
            UDTValue.fromMap(Map(
              "movie_id" -> id,
              "movie_title" -> title,
              "similarity" -> similarity
            ))
          }.toSet
          (movieId, movieIdsToTitles.getOrElse(movieId, "WTF"), movieSet)
      }

      udtSimilarities.saveToCassandra("movies", "similar_movies", SomeColumns("movie_id", "movie_title", "similar_movies"))
  }
}
