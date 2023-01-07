package com.finaly.spark
import akka.actor.ActorSystem
import akka.http.impl.util._
import akka.stream.ActorMaterializer
import akka.stream.stage.GraphStageLogic._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import lastapi._
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
import com.twitter.algebird.{TopPctCMS, TopPctCMSMonoid}
import org.apache.spark.streaming.Seconds
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.util.sketch.CountMinSketch
import spray.json._

import scala.util.parsing.json.JSON

object proj {
  case class DataObject(date: String, word: String, latLng: String, freq: Int)

  object DataObjectJsonProtocol extends DefaultJsonProtocol {
    implicit val dataObjectFormat = jsonFormat4(DataObject)
  }

  def reduceByKey(wordPair: org.apache.spark
  .streaming.dstream.DStream[(String, Int)]):
  org.apache.spark.streaming.dstream
  .DStream[(String, Int)] = {
    val wordCounts = wordPair.reduceByKey((a, b) => (a + b))
    wordCounts
  }

  def updateSketchWD(newData: Seq[(String, String)], sketch: CountMinSketch): CountMinSketch = {
    newData.foreach { case (word, date) => sketch.add(s"$word-$date", 1) }
    sketch
  }
 val arrayOfObject=Array[DataObject]();
  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    //
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)
    sc.stop()
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("/c/")
    val word="http"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val updateFunc =
      (values: Seq[Int],state: Option[Int])=>{
        val currentCount = values.foldLeft(0)(_+_)
        val previousCount = state.getOrElse(0)
        Some(currentCount + previousCount)
      }
    val topics = Array("test1")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    println("start stream")
    val sketch =CountMinSketch.create(0.1, 0.1, 1)
    val sketchWD =CountMinSketch.create(0.1, 0.1, 1)

    val lines = stream.map(_.value)

    val words = lines.map(f => f.split(","))
      .map(f => (f(1), (f(0), (f(2) + ";" + f(3)))))
      .map(f => ((f._1.replace("text", "").replaceAll("[^a-zA-Z]+", " ")), (f._2._1.replaceAll("\"", "").split(' '), f._2._2.replaceAll("[^0-9.;-]", ""))))
      .map(f => (f._1, (f._2._1(1) + "-" + f._2._1(2) + "-" + f._2._1(5), f._2._2)))
      .map(f => f._1.replace(" ", " " + f._2._1 + " " + f._2._2 + ","))

      .map(f=>f.split(","))
      // .filter(f=>((f._1 != null) && (f._1.length > 3)))
      .flatMap(words => words.map(word => (word, 1)))
      .filter(w=>((w._1.split(" ")(0).length>3)&&(w._1.split(" ").length>2)))






    val wordCounts = reduceByKey(words)
    val updateWordCounts = wordCounts
      .updateStateByKey(updateFunc)

    updateWordCounts.foreachRDD { rdd =>
      rdd.foreach { case (word, count) =>
        sketch.add(word, count)
      }
      rdd.map({ case (word, count) =>
        val estimatedCount = sketch.estimateCount(word)
        (word, estimatedCount)
      })
    }



    //top freq
    val sortedDStream = updateWordCounts.transform { rdd =>
      rdd.sortBy(_._2, ascending = false)
    }
    sortedDStream.foreachRDD{rdd=>

     val date1=rdd.map(w=>w._1.split(" ")(1)).take(5)
      val word1=rdd.map(w=>w._1.split(" ")(0)).take(5)
      val loc=rdd.map(w=>w._1.split(" ")(2)).take(5)
      val freq=rdd.map(w=>w._2).take(5)
      val opject=new DataObject(date1(0),word1(0),loc(0),freq(0))
      import DataObjectJsonProtocol._

      val jsonString = opject.toJson.compactPrint
     /* val route =
        path("data") {
          get {

            complete(HttpEntity(ContentTypes.`application/json`, s"""{"data": "$jsonString"}"""))
          }
        }*/
    }
    sortedDStream.print(5)




    ssc.start()
    ssc.awaitTermination()

  }
}

/*
let dataObj = [
{date:" ",word:" ",cord:"",freq: }]
{
  date: "10-16-2021",// [new Date('10-16-2021'), new Date('10-17-2021'), new Date('10-20-2021'), new Date('10-23-2021'), new Date('10-27-2021'), new Date('10-29-2021')]
  wordsData: [{ word: "Pale", Freq: 5, latLng: [55, 45] }, { word: "Pale", Freq: 5, latLng: [55, 45] }, { word: "Pale", Freq: 5, latLng: [55, 45] }]
},
{
  date: "10-16-2021",// [new Date('10-16-2021'), new Date('10-17-2021'), new Date('10-20-2021'), new Date('10-23-2021'), new Date('10-27-2021'), new Date('10-29-2021')]
  wordsData: [{ word: "Pale", Freq: 5, latLng: [55, 45] }, { word: "Pale", Freq: 5, latLng: [55, 45] }, { word: "Pale", Freq: 5, latLng: [55, 45] }]
}
];
}
}
*/