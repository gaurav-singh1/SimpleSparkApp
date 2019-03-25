package SparkApps

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
  * Created by farmguide on 1/7/17.
  */
object RddSample {




  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("word count").setMaster("local")
    conf.set("spark.eventLog.enabled","true")

    val sc = new SparkContext(conf)
    val input = sc.textFile("/home/farmguide/derby.log")
    val words = input.flatMap(line => line.split(" "))
    //val count: RDD[(String, Int)] = words.map(word => (word, 1))

    //val result = count.reduceByKey{case(x, y) => x + y}
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}


    counts.foreach(println)
    sc.stop()

  }



}
