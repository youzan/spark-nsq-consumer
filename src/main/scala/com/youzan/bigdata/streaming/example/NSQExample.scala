package com.youzan.bigdata.streaming.example

import java.io.FileInputStream
import java.util.Properties

import com.youzan.bigdata.streaming.nsq.{BaseMessageHandler, NsqInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by chenjunzou on 2017/3/18.
  */
object NSQExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("NSQ test")
        .setIfMissing("spark.master", "local[2]") //for console debug

    if (args.length < 1) {
      print ("Usage main.class resource")
      System.exit(-1)
    }

    val config = args(0)
    val prop = new Properties()

    prop.load(new FileInputStream(config))
    //val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(5))

    //ssc.checkpoint("checkpoint")
    val source = new NsqInputDStream(ssc, prop)
    source.map(msgWraper => msgWraper.getMessage.getReadableContent).foreachRDD(
      rdd => print(rdd.count())
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
