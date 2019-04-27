import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.collection.mutable.Set
import scala.collection.mutable.ListBuffer

object BloomFiltering{
  val S = 100
  var N = 0
  var tweets_set = Set[String]()
  var bloom_filter = new Array[Integer](32)
  var correct_estimation = 0
  var incorrect_estimation = 0

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


    tweets.foreachRDD(tweets => processTweets(tweets))
    streaming_context.start()
    streaming_context.awaitTermination()
  }


  def processTweets(tweets: RDD[Status]):Unit= {
    val tweets_collection = tweets.collect()


    for (tweet <- tweets_collection) {
      //let's get the hashtags for this tweet
      val hash_tags = tweet.getHashtagEntities().map(x => x.getText())

      //for each hashcode in the tweet, let's check if we have seen it before
      hash_tags.foreach(hash_tag => {
        //let's get the hashcode of the string
        val int_hash = hash_tag.hashCode
        //let's apply the hash functions to the hashcode
        val first_hash_function_val = h1(int_hash)
        val second_hash_function_val = h2(int_hash)

        //let's check if the bloom filter has seen these values
        if (bloom_filter(first_hash_function_val) == 1 && bloom_filter(second_hash_function_val) == 1) {

          //if it has, then let's check if the hashcode is indeed in the set of hash tags
          //if it is in the set, then it's a correct estimate, otherwise it's not
          if (tweets_set.contains(hash_tag)) {
            correct_estimation = correct_estimation + 1
          } else {
            incorrect_estimation = incorrect_estimation + 1
          }
        } else {
          bloom_filter(first_hash_function_val) = 1
          bloom_filter(second_hash_function_val) = 1
          tweets_set += hash_tag
        }

//        //let's do the same for the second hash function
//        if (bloom_filter(second_hash_function_val) == 1) {
//          if (tweets_set.contains(hash_tag)) {
//            correct_estimation = correct_estimation + 1
//          } else {
//            incorrect_estimation = incorrect_estimation + 1
//          }
//        } else {
//          bloom_filter(second_hash_function_val) = 1
//          tweets_set += hash_tag
//        }
      })
    }
    println("total unique hash tags seen so far: "+tweets_set.size)
    println("the number of correct estimates: "+correct_estimation)
    println("the number of incorrect estimates: "+incorrect_estimation)
    println("the number of false positives is: "+incorrect_estimation)
    println()
  }

  def h1(hashtag:Int):Int={
    if(hashtag < 0)
      return -hashtag % 10
    return hashtag % 10
  }

  def h2(hashtag:Int):Int={
    if(hashtag < 0)
      return -hashtag % 30
    return hashtag % 30
  }

}
