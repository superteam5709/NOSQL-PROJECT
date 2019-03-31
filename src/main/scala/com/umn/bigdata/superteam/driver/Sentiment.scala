package com.umn.bigdata.superteam.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import twitter4j.Status
import org.apache.spark.sql.cassandra._

object Sentiment {


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

    val cassandraKeyspaceRussia: String = spark.conf.get("spark.mycassandra.keyspacerussia")

    val cassandraTableRussia: String = spark.conf.get("spark.mycassandra.tablerussia")

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


    //    val AFINN = sc.textFile("/Users/hectorcamarena/Documents/AFINN.txt").map(x=> x.split("\t")).map(x=>(x(0).toString,x(1).toInt))

    val AFINN = sc.textFile("/home/cass/sandbox/AFINN.txt").map(x=> x.split("\t")).map(x=>(x(0).toString,x(1).toInt))


    val tweetsWordMatch = englishTweets
      .filter(x =>
        //x.getPlace.getCountryCode() == "US" &&
        x.getText.toLowerCase.contains("communism") ||
          x.getText.toLowerCase.contains("putin") ||
          x.getText.toLowerCase.contains("russia")
      )

    tweetsWordMatch.foreachRDD(rdd =>{
      println("======================= WINDOW ============================")
      import spark.implicits._
      val rddIDandText = rdd.map(r => (r.getId,r.getText))
      val rddToArray = rddIDandText.toDF("id","text").collect()
      //        val newrdd = rddIDandText.toDF("score", "text")
      //        newrdd.show(100)


      val tweetsSenti = rddToArray.map(tweetText => {
        val tweetWordsSentiment = tweetText(1).toString.split(" ").map(word => {
          var senti: Int = 0
          if (AFINN.lookup(word.toLowerCase()).length > 0) {
            senti = AFINN.lookup(word.toLowerCase())(0)
          }
          senti
        })
        val tweetSentiment = tweetWordsSentiment.sum
        (tweetSentiment, tweetText.toString)
      })

      val tweetsSentiRDD: org.apache.spark.rdd.RDD[(Int, String)] = sc.parallelize(tweetsSenti.toList).sortBy(x => x._1, false)
      tweetsSentiRDD.foreach(println(_))
      println()
      println("*****print russia rating******")
      println()
      tweetsSentiRDD.map(rdd => (rdd._1,rdd._2.split(",")(0),rdd._2.split(",")(1))).toDF("rating","id","text").write.mode("append").cassandraFormat(cassandraTableRussia,cassandraKeyspaceRussia).save()
    })

    val trumpsWordMatch = englishTweets
      .filter(x =>
        //x.getPlace.getCountryCode() == "US" &&
        x.getText.toLowerCase.contains("trump") ||
          x.getText.toLowerCase.contains("realdonaldtrump") ||
          x.getText.toLowerCase.contains("maga") ||
          x.getText.toLowerCase.contains("potus")
      )

    trumpsWordMatch.foreachRDD(rdd =>{
      println("======================= WINDOW ============================")
      import spark.implicits._
      val rddIDandText = rdd.map(r => (r.getId,r.getText))
      val rddToArray = rddIDandText.toDF("id","text").collect()
      //        val newrdd = rddIDandText.toDF("score", "text")
      //        newrdd.show(100)


      val tweetsSenti = rddToArray.map(tweetText => {
        val tweetWordsSentiment = tweetText(1).toString.split(" ").map(word => {
          var senti: Int = 0
          if (AFINN.lookup(word.toLowerCase()).length > 0) {
            senti = AFINN.lookup(word.toLowerCase())(0)
          }
          senti
        })
        val tweetSentiment = tweetWordsSentiment.sum
        (tweetSentiment, tweetText.toString)
      })

      val tweetsSentiRDD: org.apache.spark.rdd.RDD[(Int, String)] = sc.parallelize(tweetsSenti.toList).sortBy(x => x._1, false)
      tweetsSentiRDD.foreach(println(_))
      println()
      println("*****printing trump rating******")
      println()
      tweetsSentiRDD.map(rdd => (rdd._1,rdd._2.split(",")(0),rdd._2.split(",")(1))).toDF("rating","id","text").write.mode("append").cassandraFormat(cassandraTable,cassandraKeyspace).save()
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(jobDuration)
  }
}
