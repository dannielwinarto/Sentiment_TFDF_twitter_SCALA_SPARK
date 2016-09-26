/**
  * Created by dan_9 on 9/20/2016.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source
import scala.util.{Try, Success, Failure}
import scala.math

object TFDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSentiment").setMaster("local[2]")
    val sc = new SparkContext(sparkConf) // create SparkContext Object
//    val Q2TFDF = sc.textFile("C:\\Users\\dan_9\\Desktop\\INF 553\\HW 1\\HW1_INF553\\shortTwitter.txt") // by default this will partition by line. the lines will distributed into partition, one partition can have 1 or more
//    val Q2TFDF = sc.textFile("C:\\Users\\dan_9\\Desktop\\INF 553\\HW 1\\HW1_INF553\\winarto_danniel_first20.txt")
    val Q2TFDF = sc.textFile(args(0))
      .map(text => text.toString.toLowerCase)
      .map(text2 => text2.split("\"text\":\"")(1).split("\"source\":\"")(0))
      .map(text2 => text2.replaceAll("""[\p{Punct}&&[^.]]""", ""))
      .map(text2 => text2.replaceAll("\\.", ""))
      .zipWithIndex
      .flatMap {
        case (k, index) => k.split(" ").map(word => ((word, index + 1), 1))
      }
      .reduceByKey(_ + _)
      .sortByKey()
      .map {
        case ((word, index), count) => (word, (index, count))
      }
//    Q2TFDF.foreach(println)

    val temp = Q2TFDF.map { // creating tuple for df
      case (word, (index, count)) => (word, 1)
    }
      .reduceByKey(_ + _)
//    temp.foreach(println)

    val temp2 = Q2TFDF.groupBy(_._1)// creating tuple for list of occurrence
      .map(kv => (kv._1, kv._2.map(_._2).toList))
//        temp2.foreach(println)

    val result = temp.join(temp2)
      .sortByKey(true, 1)
      .map {
        case (k, (i, j)) => (k, i, j) // map to just fix into proper format
      }
    result.foreach(println)
    result.saveAsTextFile("winarto_danniel_tweets_tfdf_first20.txt")
  }
}
