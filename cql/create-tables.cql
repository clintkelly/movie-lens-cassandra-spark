DROP KEYSPACE IF EXISTS movies;
CREATE KEYSPACE movies WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use movies;

/*
  Time-series data of movie reviews.

  We may want to display this data in a timeline, so we include the movie title and URL here also.
*/
CREATE TABLE reviews (
  user_id bigint,
  review_time timestamp,
  movie_id bigint,
  movie_title text,
  movie_url text,
  rating int,
  PRIMARY KEY ((user_id), review_time)
) WITH CLUSTERING ORDER BY (review_time DESC);

CREATE TYPE user_info (
  age int,
  gender text,
  occupation text,
  zipcode text
);

CREATE TABLE user_metadata (
  user_id bigint PRIMARY KEY,
  info FROZEN <user_info>
);

CREATE TABLE movie_metadata (
  movie_id bigint PRIMARY KEY,
  title text,
  url text,
  genres set<text>
);

CREATE TYPE similar_movie (
  movie_id bigint,
  movie_title text,
  similarity double
);

CREATE TABLE similar_movies (
  movie_id bigint PRIMARY KEY,
  movie_title text,
  similar_movies set< FROZEN<similar_movie> >
);

