import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._

import scala.collection.mutable.HashMap

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf

object KeywordTweets extends App {

  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.ERROR)
  // Configure Twitter credentials
  val apiKey = "WNYbNfoKvE12Gc8178zpVuzWp"
  val apiSecret = "sGZ8jFkWZxYsnPIDloYjo3gNfPmsadA5HQjEhRO2Rmy6kITj8I"
  val accessToken = "217221545-RGl2qvVsYtcPetOTcoS8Q71VK0yxWUI2mjufcv7p"
  val accessTokenSecret = "svbYOQYNBv4Hw3SAbOJcvBEMEkRpGSJjzHBg83Sq3CsUp"
  configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)

  val sc = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterDataProcessor")
  val ssc = new StreamingContext(sc, Seconds(15))
  val stream = TwitterUtils.createStream(ssc, None)

  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

  val topCounts120 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(120)).map{case (topic, count) => (count, topic)}.transform(_.sortByKey(false))
  val topCounts30 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30)).map{case (topic, count) => (count, topic)}.transform(_.sortByKey(false))

  // Print popular hashtags
  topCounts120.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 120 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })

  topCounts30.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 30 seconds (%s total):".format(rdd.count()))
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
  })

  topCounts120.saveAsTextFiles("/home/sangeles/Byte/BIGDATA/Twitter120Seconds/")
  topCounts30.saveAsTextFiles("/home/sangeles/Byte/BIGDATA/Twitter30Seconds/")

  // run forever
  ssc.start()
  ssc.awaitTermination()

  def configureTwitterCredentials(apiKey: String, apiSecret: String, accessToken: String, accessTokenSecret: String) {
    val configs = new HashMap[String, String] ++= Seq(
      "apiKey" -> apiKey, "apiSecret" -> apiSecret, "accessToken" -> accessToken, "accessTokenSecret" -> accessTokenSecret)
    println("Configuring Twitter OAuth")
    configs.foreach{ case(key, value) =>
      if (value.trim.isEmpty) {
        throw new Exception("Error setting authentication - value for " + key + " not set")
      }
      val fullKey = "twitter4j.oauth." + key.replace("api", "consumer")
      System.setProperty(fullKey, value.trim)
      println("\tProperty " + fullKey + " set as [" + value.trim + "]")
    }
    println()
  }
}
