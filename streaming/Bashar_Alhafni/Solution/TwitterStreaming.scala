import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object TwitterStreaming {
  val S = 100
  var N = 0
  var tweet_list = new ListBuffer[Status]()
  var hashtags_map = mutable.HashMap[String, Int]()
  var tweet_length = 0

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel(logLevel = "OFF")

    val consumerKey = "UdRfUeSWSLWqcD0FxZXDhgHHT"
    val consumerSecret ="fa2gZFA9CA1F422o3BX5ClPhPfPaN0QNJ04C1hd8ZQSGmVYrkK"
    val accessToken ="1063986211881009153-VA6jBLQDdBV0BnMykWRy1gElXn2FG9"
    val accessTokenSecret="mDjA9HRt3eKmAlRTQdv6kd8XRgudbG6hnx6X0APNwJMqh"

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val streaming_context = new StreamingContext(sc, Seconds(10))

    val tweets = TwitterUtils.createStream(streaming_context, None, Array("Data"))


    tweets.foreachRDD(tweets => reservoirSampling(tweets))
    streaming_context.start()
    streaming_context.awaitTermination()
  }

  def printTweet(tweets:RDD[Status]):Unit={
    val tweets_collection = tweets.collect()
    tweets_collection.foreach(tweet => println(tweet))
//    print(tweet)
  }

  def reservoirSampling(tweets: RDD[Status]):Unit={
    val tweets_collection = tweets.collect()

    for(tweet <- tweets_collection){
      if( N < S){
        //we just add the tweet to the list
        tweet_list += tweet

        //let's get the hashtags of this tweet
        val hashtags = tweet.getHashtagEntities().map(x => x.getText)
        //mainting the count of a particular hashtag
        hashtags.foreach(hashtag => {
          if(hashtags_map.contains(hashtag)){
            hashtags_map.put(hashtag, hashtags_map.apply(hashtag) + 1)
          }else{
            hashtags_map.put(hashtag, 1)
          }
        })
        //let's add the tweet length to the total length
        tweet_length = tweet_length + tweet.getText().length()
      }
        //if N>S, we keep the nth element otherwise, we discard it
        //if we picked the nth element, then it will replace one of the
        //elements in the list, picked uniformly at random
      else{
        //let's pick a tweet at random with a probability of 100/n
        val index = Random.nextInt(N)
        //if index > S (i.e. we discard the element) else we add it
        if(index < S){
          //let's get the tweet
          val discarded_tweet = tweet_list(index)

          //let's add the new incoming tweet to the list
          tweet_list(index) = tweet


          //let's get rid of the hashtags belonging to the discarded tweet
          val hashtags_discarded = discarded_tweet.getHashtagEntities().map(x => x.getText())
          hashtags_discarded.foreach(hashtag=>{
            hashtags_map.put(hashtag,  hashtags_map.apply(hashtag) - 1)
          })

          //let's remove the discarded tweet length from the tweet length
          tweet_length = tweet_length - discarded_tweet.getText().length()

          //let's add the hashtags belonging to the new tweet
          val hashtags_new = tweet.getHashtagEntities().map(x => x.getText())
          hashtags_new.foreach(hashtag =>{
            if(hashtags_map.contains(hashtag)){
              hashtags_map.put(hashtag, hashtags_map.apply(hashtag) + 1)
            }else{
              hashtags_map.put(hashtag, 1)
            }
          })

          //let's add the new tweet length to the tweet length
          tweet_length = tweet_length + tweet.getText().length()

          //let's sort hashtags_map by count and emit the top 5 hashtags
          val hot5tags = hashtags_map.toList.sortBy(x => x._2).take(5)
          println("The number of the twitter from beginning: "+N)
          println("Top 5 hot hashtags:")
          hot5tags.foreach(tag=>{
            println(tag._1+": "+tag._2)
          })
          println("the average length of the twitter is: "+tweet_length/N.toDouble)
          println()
          println()
          }
        }
      N = N +1
    }
  }
}
