#!/usr/bin/env bash

# run jobs individually like this:
spark-submit --properties-file tweetApp-tweetCount.properties --class com.umn.bigdata.superteam.driver.TweetCount NoSQLProject-1.0-SNAPSHOT.jar

spark-submit --properties-file tweetApp-tweetCount.properties --class com.umn.bigdata.superteam.driver.TweetLocation NoSQLProject-1.0-SNAPSHOT.jar

spark-submit --properties-file tweetApp-tweetCount.properties --class com.umn.bigdata.superteam.driver.Sentiment NoSQLProject-1.0-SNAPSHOT.jar

spark-submit --properties-file tweetApp-tweetCount.properties --class com.umn.bigdata.superteam.driver.TopTrending NoSQLProject-1.0-SNAPSHOT.jar

spark-submit --properties-file tweetApp-tweetCount.properties --class com.umn.bigdata.superteam.driver.TopRetweeter NoSQLProject-1.0-SNAPSHOT.jar

