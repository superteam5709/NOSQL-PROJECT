# NOSQL-PROJECT

Below is are the commands we used to create tables corresponding to each class.

keyspace demo:
CREATE KEYSPACE demo
    WITH REPLICATION = {
        'class': 'SimpleStrategy', 
        'replication_factor': 1
    };
----------------------------------
tweet:
CREATE TABLE demo.tweet (
    id bigint PRIMARY KEY
) ;
----------------------------------
distinct_user:
CREATE TABLE demo.distinct_user (
    user text PRIMARY KEY,
    count counter
) ;
----------------------------------
hashtag:
CREATE TABLE demo.hashtag (
    tags text PRIMARY KEY,
    freq counter
) ;
----------------------------------
toptweeter:
CREATE TABLE demo.toptweeter (
    top_user text PRIMARY KEY,
    retweeted counter
) ;
----------------------------------
location:
CREATE TABLE demo.location (
    id bigint PRIMARY KEY,
    latitude float,
    longitude float
) 
----------------------------------
trump_sentiment:
CREATE TABLE demo.trump_sentiment (
    id text PRIMARY KEY,
    rating int,
    text text
) ;
----------------------------------
russia_sentiment:
CREATE TABLE demo.russia_sentiment (
    id text PRIMARY KEY,
    rating int,
    text text
) ;
----------------------------------
status_hifi:
CREATE TABLE demo.status_hifi (
    id bigint PRIMARY KEY,
    created_at text,
    entities list<text>,
    extended_entities list<text>,
    favorite_count int,
    favorited boolean,
    in_reply_to_screen_name text,
    in_reply_to_status_id bigint,
    in_reply_to_user_id bigint,
    lang text,
    latitude float,
    longitude float,
    place_id text,
    possibly_sensitive boolean,
    quoted_status text,
    quoted_status_id bigint,
    retweet_count int,
    retweeted boolean,
    retweeted_status text,
    text text,
    truncated boolean,
    user_id bigint
) ;
----------------------------------
user_hifi:
CREATE TABLE demo.user_hifi (
    id bigint PRIMARY KEY,
    contributors_enabled boolean,
    created_at text,
    default_profile boolean,
    default_profile_image boolean,
    description text,
    favourites_count int,
    follow_request_sent boolean,
    followers_count int,
    friends_count int,
    geo_enabled boolean,
    lang text,
    listed_count int,
    location text,
    name text,
    profile_image_url text,
    protected boolean,
    screen_name text,
    statuses_count int,
    url text,
    verified boolean,
    withheld_in_countries text
);
----------------------------------
place_hifi:
CREATE TABLE demo.place_hifi (
    id text PRIMARY KEY,
    bounding_box frozen<tuple<frozen<list<text>>, text>>,
    country text,
    country_code text,
    full_name text,
    name text,
    place_type text,
    url text
) ;
==================================
***Enabiling beeline + hive jdbc + spark sql***
[cass@superteam ~]$ beeline
Beeline version 1.2.1.spark2 by Apache Hive
beeline>
beeline> !connect jdbc:hive2://localhost:10000
Connecting to jdbc:hive2://localhost:10000
Enter username for jdbc:hive2://localhost:10000: cass
Enter password for jdbc:hive2://localhost:10000:
log4j:WARN No appenders could be found for logger (org.apache.hive.jdbc.Utils).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Connected to: Spark SQL (version 2.2.0)
Driver: Hive JDBC (version 1.2.1.spark2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
0: jdbc:hive2://localhost:10000>
==================================
***Create (bring) cassandra tables from demo keyspace to the Spark Thrift Server (so that Tableau can see these tables)***
CREATE TABLE demo_tweet using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "tweet");
CREATE TABLE demo_distinct_user using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "distinct_user");
CREATE TABLE demo_hashtag using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "hashtag");
CREATE TABLE demo_toptweeter using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "toptweeter");
CREATE TABLE demo_location using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "location");
CREATE TABLE demo_trump_sentiment using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "trump_sentiment");
CREATE TABLE demo_russia_sentiment using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "russia_sentiment");
CREATE TABLE demo_status_hifi using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "status_hifi");
CREATE TABLE demo_user_hifi using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "user_hifi");
CREATE TABLE demo_place_hifi using org.apache.spark.sql.cassandra OPTIONS (keyspace "demo", table "place_hifi");
