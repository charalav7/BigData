package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import com.datastax.driver.core.Cluster
import org.apache.spark.streaming.twitter.TwitterUtils
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object TwitterSpark {
  def main(args: Array[String]): Unit = {
    /*
      Connect to Cassandra
    */

    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    // create twitter_space keyspace - create a table with hashtag, count, positive, negative, neutral columns - and finally close session
    session.execute("CREATE KEYSPACE IF NOT EXISTS twitter_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS twitter_space.twitter (hashtag text PRIMARY KEY, count int, positive int, negative int, neutral int);")
    session.close()

    /*
      Make a connection to Twitter and read (key, value) pairs from it
    */

    // define spark configuration by setting the app name and the number of local threads
    val sparkConf = new SparkConf()
      .setAppName("TwitterSpark")
      .setMaster("local[2]")

    // create a streaming context with a 5 second batch size - create a checkpoint
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // necessary to create a checkpoint as we will use mapWithState function
    ssc.checkpoint(".")

    // define connection properties with Twitter
    val consumerKey = "<>"
    val consumerSecret = "<>"
    val accessToken = "<>"
    val accessTokenSecret = "<>"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // create a DStream for the specified streaming context with TwitterUtils class
    val tStream = TwitterUtils.createStream(ssc, None)

    // keep only the tweets with english language that include hashtags
    val tweets = tStream
      .filter(_.getLang == "en")
      .filter { tweet =>
        val hashtags = tweet
          .getText
          .split(" ")
          .filter(_.startsWith("#"))
        hashtags.exists { h => true}
      }

    // perform sentiment analysis for each tweet
    // extract hashtags from the stream
    // define the (hashtag, (count, pos, neg, neu)) pairs for each tweet
    val tags = tweets
      .flatMap { tweet =>
        val sentiment = SentimentAnalyzer.getSentimentCategory(tweet.getText)
        tweet
          .getHashtagEntities
          .map(_.getText)
          .map(_.toLowerCase)
          .map { tag =>
            sentiment match {
              case "Positive" => (tag, (1, 1, 0, 0))
              case "Negative" => (tag, (1, 0, 1, 0))
              case "Neutral" => (tag, (1, 0, 0, 1))
            }
          }
      }

    /*
      Calculate the new counter values in accordance with their all time old values in the stream
      It will be given to updateStateByKey method
    */

    val mappingFunc = (newValues: Seq[(Int, Int, Int, Int)], oldValues: Option[(Int, Int, Int, Int)]) => {
      val safeOldValues = oldValues.getOrElse((0, 0, 0, 0))
      var newCount = safeOldValues._1
      var newPos = safeOldValues._2
      var newNeg = safeOldValues._3
      var newNeu = safeOldValues._4
      for (item <- newValues) {
        newCount += item._1
        newPos += item._2
        newNeg += item._3
        newNeu += item._4
      }
      Some ((newCount, newPos, newNeg, newNeu))
    }

    // create a new DStream in a stateful manner
    val stateDstream = tags.updateStateByKey(mappingFunc)

    // print results just for demonstration
    stateDstream.foreachRDD { rdd =>
      // sort by the the total number of occurrences - keep the 10 most popular tags
      val topList = rdd.sortBy( f => f._2._1, ascending = false).take(10)
      topList.foreach { case (tag, count) =>
        println("#%s -> %s times | %s positive | %s negative | %s neutral,".format(tag, count._1, count._2, count._3, count._4))
      }
    }

    /*
      Cassandra procedure
    */

    // from stateDstream take only the 10 most popular entries of each RDD and map it in a tuple form to be able to save to Cassandra
    val cassandraDstream = stateDstream
      .transform( rdd => {
        rdd
          .filter(rdd.sortBy( f => f._2._1, ascending = false).take(10).toList.contains)
          .map ( pair => (pair._1, pair._2._1, pair._2._2, pair._2._3, pair._2._4))
      })

    // save to cassandra
    cassandraDstream.saveToCassandra("twitter_space", "twitter", SomeColumns("hashtag", "count", "positive", "negative", "neutral"))

    /*
      Start streaming context to perform all the computations
    */
    ssc.start()
    // terminate after 1 hour i.e. 3600000 msec
    ssc.awaitTerminationOrTimeout(3600000)
    ssc.stop()}
}
