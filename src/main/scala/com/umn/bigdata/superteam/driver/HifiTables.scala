package com.umn.bigdata.superteam.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import org.apache.spark.sql.cassandra._

object HifiTables{

  def main(args: Array[String]): Unit = {

    //Set keys with twitter developer keys
    System.setProperty("twitter4j.oauth.consumerKey", "")
    System.setProperty("twitter4j.oauth.consumerSecret", "")
    System.setProperty("twitter4j.oauth.accessToken", "")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "")
    // Create a spark session
    val spark = SparkSession.builder().master("local[*]").appName("twitter-stream").getOrCreate()

    // Create a spark context
    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("Error")


    // Create a streaming context
    val ssc = new StreamingContext(sc, Seconds(1))

    // Create a tweet stream
    val tweets: DStream[Status] = TwitterUtils.createStream(ssc, None)
    // Filter by English language
    val englishTweets = tweets.filter(_.getLang() == "en")


    val place_hifi = englishTweets.map(x => (
      Option(x.getPlace).map(_.getId).getOrElse(default = x.getId.toString),
      // (Option(x.getPlace).map(_.getBoundingBoxCoordinates.toString).getOrElse(default = "-.-"),
      // Option(x.getPlace).map(_.getBoundingBoxType).getOrElse(default = null)),
      Option(x.getPlace).map(_.getCountry).getOrElse(default = null),
      Option(x.getPlace).map(_.getCountryCode).getOrElse(default = null),
      Option(x.getPlace).map(_.getFullName).getOrElse(default = null),
      Option(x.getPlace).map(_.getName).getOrElse(default = null),
      Option(x.getPlace).map(_.getPlaceType).getOrElse(default = null),
      Option(x.getPlace).map(_.getURL).getOrElse(default = null)

    ))

    place_hifi
      .foreachRDD {
        rdd =>
          import spark.implicits._
          val rddToDF = rdd.toDF(
            "id",
            // "bounding_box",
            "country",
            "country_code",
            "full_name",
            "name",
            "place_type",
            "url"
          )
          println("inserting into place_hifi table")
          rddToDF.show(100)
          rddToDF.write.mode("append").cassandraFormat("place_hifi","demo").save()

      }

    val users_hifi = englishTweets.map(x => (
      x.getUser.getId,
      x.getUser.isContributorsEnabled,
      x.getUser.getCreatedAt.toString,
      x.getUser.isDefaultProfile,
      x.getUser.isDefaultProfileImage,
      Option(x.getUser.getDescription).
        getOrElse(default = x.getUser.getDescription),
      x.getUser.getFavouritesCount,
      Option(x.getUser.isFollowRequestSent).
        getOrElse(default = x.getUser.isFollowRequestSent),
      x.getUser.getFollowersCount,
      x.getUser.getFriendsCount,
      x.getUser.isGeoEnabled,
      x.getUser.getLang,
      x.getUser.getListedCount,
      Option(x.getUser.getLocation).getOrElse(default = x.getUser.getLocation),
      x.getUser.getName,
      x.getUser.getProfileImageURL,
      x.getUser.isProtected,
      x.getUser.getScreenName,
      x.getUser.getStatusesCount,
      Option(x.getUser.getURL).getOrElse(default = x.getUser.getURL),
      x.getUser.isVerified,
      x.getUser.getWithheldInCountries
    ))

    users_hifi
      .foreachRDD {
        rdd =>
          import spark.implicits._
          val rddToDF = rdd.toDF(
            "id",
            "contributors_enabled",
            "created_at",
            "default_profile",
            "default_profile_image",
            "description",
            "favourites_count",
            "follow_request_sent",
            "followers_count",
            "friends_count",
            "geo_enabled",
            "lang",
            "listed_count",
            "location",
            "name",
            "profile_image_url",
            "protected",
            "screen_name",
            "statuses_count",
            "url",
            "verified",
            "withheld_in_countries"
          )
          println("inserting into users_hifi table")
          rddToDF.show(100)
          rddToDF.write.mode("append").cassandraFormat("user_hifi","demo").save()

      }

    val status_hifi = englishTweets.map(x => (
      x.getId,
      Option(x.getGeoLocation).map(_.getLongitude) match {
        case Some(_) => x.getGeoLocation.getLongitude
        case None => 181.0
      },
      Option(x.getGeoLocation).map(_.getLatitude) match {
        case Some(_) => x.getGeoLocation.getLatitude
        case None => 91.0
      },
      x.getCreatedAt.toString,
      x.getHashtagEntities.map(_.getText),
      x.getExtendedMediaEntities.map(_.getExpandedURL),
      Option(x.getFavoriteCount).getOrElse(default = x.getFavoriteCount),
      Option(x.isFavorited).getOrElse(default = x.isFavorited),
      Option(x.getInReplyToScreenName).getOrElse(default = x.getInReplyToScreenName),
      Option(x.getInReplyToStatusId).getOrElse(default = x.getInReplyToStatusId),
      Option(x.getInReplyToUserId).getOrElse(default = x.getInReplyToUserId),
      Option(x.getLang).getOrElse(default = x.getLang),
      Option(x.getPlace).map(_.getId).getOrElse(default = null),
      Option(x.isPossiblySensitive).getOrElse(default = x.isPossiblySensitive),
      Option(x.getQuotedStatus).map(_.getText).getOrElse(default = null),
      Option(x.getQuotedStatusId).getOrElse(default = x.getQuotedStatusId),
      Option(x.getRetweetCount).getOrElse(default = x.getRetweetCount),
      Option(x.isRetweeted)getOrElse(default = x.isRetweeted),
      Option(x.getRetweetedStatus).map(_.getText).getOrElse(default = null),
      // Option(x.getSource).getOrElse(default = x.getSource),
      Option(x.getText).getOrElse(default = x.getText),
      Option(x.isTruncated).getOrElse(default = x.isTruncated),
      Option(x.getUser).map(_.getId).getOrElse(default = x.getUser.getId)
    ))
    //      status_hifi.print()

    status_hifi
      .foreachRDD {
        rdd =>
          import spark.implicits._
          val rddToDF = rdd.toDF(
            "id",
            "longitude",
            "latitude",
            "created_at",
            "entities",
            "extended_entities",
            "favorite_count",
            "favorited",
            "in_reply_to_screen_name",
            "in_reply_to_status_id",
            "in_reply_to_user_id",
            "lang",
            "place_id",
            "possibly_sensitive",
            "quoted_status",
            "quoted_status_id",
            "retweet_count",
            "retweeted",
            "retweeted_status",
            // "source",
            "text",
            "truncated",
            "user_id")
          println("inserting into status_hifi table")
          rddToDF.show(100)
          rddToDF.write.mode("append").cassandraFormat("status_hifi","demo").save()

      }

    ssc.start()
    ssc.awaitTermination()
  }
}