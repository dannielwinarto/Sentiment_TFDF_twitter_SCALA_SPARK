/**
  * Created by dan_9 on 9/18/2016.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.io.Source
import scala.util.{Try, Success, Failure}
import scala.math

object Sentiment {
  def main(args: Array[String]): Unit = {
//    val filename = "C:\\Users\\dan_9\\Desktop\\INF 553\\HW 1\\HW1_INF553\\AFINN-111.txt"
    val filename = args(1)
    var MapperDict = scala.collection.mutable.Map[String, String]()
    for (line <- Source.fromFile(filename).getLines) {
      var temp = line.split("\t")
      MapperDict += (temp(0) -> temp(1))
    }

    val sparkConf = new SparkConf().setAppName("SparkSentiment").setMaster("local[2]")
    val sc = new SparkContext(sparkConf) // create SparkContext Object
//    val text = sc.textFile("C:\\Users\\dan_9\\Desktop\\INF 553\\HW 1\\HW1_INF553\\shortTwitter.txt") // by default this will partition by line. the lines will distributed into partition, one partition can have 1 or more
//    val text = sc.textFile("C:\\Users\\dan_9\\Desktop\\INF 553\\HW 1\\HW1_INF553\\winarto_danniel_first20.txt")
    val text = sc.textFile(args(0))
      .map(text => text.toString.toLowerCase)
      .map(text2 => text2.split("\"text\":\"")(1).split("\"source\":\"")(0))
      .map(text2 => text2.replaceAll("""[\p{Punct}&&[^.]]""", ""))
      .map(text2 => text2.replaceAll("\\.", ""))
      .zipWithIndex
      .flatMap {
        case (k, index) => k.split(" ").map(word => Try((index + 1).toInt, MapperDict(word).toInt).getOrElse((index + 1).toInt, 0))
      }
      .reduceByKey(_+_)
      .sortByKey(true,1)
    text.foreach(println)
    text.saveAsTextFile("winarto_danniel_tweets_sentiment_first20")
  }
}
