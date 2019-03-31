package com.umn.bigdata.superteam.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import twitter4j.Status
import org.apache.spark.sql.cassandra._

object TweetCount {


  def main(args: Array[String]): Unit = {
    //Set keys with twitter developer keys
    System.setProperty("twitter4j.oauth.consumerKey", "")
    System.setProperty("twitter4j.oauth.consumerSecret", "")
    System.setProperty("twitter4j.oauth.accessToken", "")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "")

    val spark = SparkSession.builder().master("local[*]")
      //.config("spark.cassandra.connection.host", "127.0.0.1")
      .appName("twitter-stream").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("Error")



    val jobDurationSeconds: Int = spark.conf.get("spark.streaming.jobDuration.inSec").toInt
    // Seconds multiplied by 1000 to get ms
    val jobDuration: Long = jobDurationSeconds * 1000

    val cassandraKeyspace: String = spark.conf.get("spark.mycassandra.keyspace")

    val cassandraTable: String = spark.conf.get("spark.mycassandra.table")

    val slideIntervalInSeconds: Int = spark.conf.get("spark.streaming.slideInterval.inSec").toInt



    // Recompute the top hashtags every 1 second
    val slideInterval = new Duration(slideIntervalInSeconds * 1000)


    // Get first argument to get jobName
    println("Preparing for stream job with Properties: ")
    println("job duration of : " + jobDurationSeconds + " seconds")
    println("Sliding Interval duration of : " + slideIntervalInSeconds + " seconds")
    println("Will save to cassandra keyspace: " + cassandraKeyspace)
    println("Will save to cassandra table: " + cassandraTable)


    // Create a Spark Streaming Context.
    val ssc = new StreamingContext(sc, slideInterval)
    // Create a tweet stream

    // Create a tweet stream
    val tweets: DStream[Status] = TwitterUtils.createStream(ssc, None)
    // Filter by English language
    val englishTweets = tweets.filter(_.getLang() == "en")
    //      englishTweets.print()
    // Using flatMap to get the unique ID of all tweets in the stream
    val id = englishTweets.flatMap {status => Traversable(status.getId)}
    //      id.print()
    id
      //foreach tweet, get the unique id and convert to rdd
      .foreachRDD { rdd =>
      import spark.implicits._
      //
      //This block of code is to turn our stream into a dataframe rdd batch and persist it to Cassandra
      val rddToDF = rdd.toDF("id")
      rddToDF.show(100)
      println("inserting into cassandra table")
      rddToDF.write.mode("append").cassandraFormat("tweet","demo").save()
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(jobDuration)
  }

}
