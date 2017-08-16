package com.youzan.bigdata.streaming.example

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.nsq.NSQInputDStream
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by chenjunzou on 2017/3/18.
  */
object NSQExample {
  def main(args: Array[String]): Unit = {
    def createContext(checkpointDirectory: String, parallism: Int)
    : StreamingContext = {

      // If you do not see this printed, that means the StreamingContext has been loaded
      // from the new checkpoint
      println("Creating new context")
      val conf = new SparkConf()
        .setAppName("NSQ test")
        .setIfMissing("spark.master", "local[2]")
        .set("spark.streaming.backpressure.enabled", "true")
        .set("spark.streaming.receiver.writeAheadLog.enable", "true")

      val config = Source.fromURL(getClass.getClassLoader.getResource("dev.properties"))
      val prop = new Properties()
      prop.load(config.bufferedReader())

      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(15))

      ssc.checkpoint(checkpointDirectory)

      val sources = (1 to parallism).map(_ => new NSQInputDStream(ssc, prop.toMap))
      val unionDStream = ssc.union(sources)

      val traceDstream = unionDStream
        .flatMap(wrapper => wrapper.getMessage.getReadableContent.split(" "))
        .map(w => {
          println(w)
          (w, 1)
        })

      // Update the cumulative count using mapWithState
      // This will give a DStream made of state (which is the cumulative count of the words)
      val mappingFunc = (word: String, inintial: Option[Int], state: State[Int]) => {
        val sum = inintial.getOrElse(0) + state.getOption.getOrElse(0)
        val output = (word, sum)
        state.update(sum)
        output
      }

      val outputDstream = traceDstream.mapWithState(
        StateSpec.function(mappingFunc))
      val stateDstream = outputDstream.stateSnapshots()

      stateDstream.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
        // Get or register the blacklist Broadcast
        // Use blacklist to drop words and use droppedWordsCounter to count them
        val words = rdd.collect.mkString("[", ", ", "]")
        val output = "Collect at time " + time + " " + words
        println(output)
      }
      ssc
    }

    if (args.length != 2) {
      System.err.println("Your arguments were " + args.mkString("[", ", ", "]"))
      System.err.println(
        """
          |Usage: NSQExample <checkpoint-directory> <parallism>.
          |     <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data
          |     <parallism> dstream receiver parallism
          |In local mode, <master> should be 'local[n]' with n > 1
          |<checkpoint-directory>  must be absolute paths
        """.stripMargin
      )
      System.exit(1)
    }
    val Array(checkpointDirectory, parallism) = args
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(checkpointDirectory, parallism.toInt))

    ssc.start()
    ssc.awaitTermination()
  }
}
