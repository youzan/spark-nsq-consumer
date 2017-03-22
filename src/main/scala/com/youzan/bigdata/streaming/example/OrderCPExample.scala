package com.youzan.bigdata.streaming.example

import java.io.FileInputStream
import java.util.Properties

import com.youzan.bigdata.streaming.{RedisMixin, StreamingBase}
import com.youzan.bigdata.streaming.cache.RedisConnPool
import com.youzan.bigdata.streaming.nsq.{NSQMessageWrapper, NsqInputDStream}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

/**
  * Created by chenjunzou on 2017/3/23.
  */
class OrderCPExample(conf: SparkConf, props: Properties)
  extends StreamingBase with RedisMixin[String] {
  val _sparkConf: SparkConf = conf
  val _props: Properties = props

  val batchInterval: Int = props.getProperty("spark.streaming.interval").toInt
  assert(batchInterval >= 1000 && batchInterval < 120000, "batch interval should range in [1, 120] seconds")
  val ssc = new StreamingContext(conf, Milliseconds(batchInterval))
  def prop: Properties = _props

  def run() = {
    val extractedSource = getSourceDStream.map(extract)
    RedisProcedure(extractedSource)

    ssc.start()
    ssc.awaitTermination()
  }

  def getSourceDStream: DStream[NSQMessageWrapper] = {
    new NsqInputDStream(ssc, prop)
  }
  override def extract: (NSQMessageWrapper => String)= {
    msg: NSQMessageWrapper => msg.getMessage.getReadableContent
  }

  override def getRedisPool(): RedisConnPool = new RedisConnPool(prop)

  override def broadcastRedisPool(): Broadcast[RedisConnPool] = ssc.sparkContext.broadcast(getRedisPool())

  override def execute: (String, Jedis) => Unit =
  {
    case (msg: String, jedis: Jedis) => {
      print(msg)
    }
  }
}

object OrderCPExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("NSQ test")
      .setIfMissing("spark.master", "local[2]") //for console debug

    val config = args(0)
    val prop = new Properties()
    prop.load(new FileInputStream(config))

    val cp: OrderCPExample = new OrderCPExample(conf, prop)

  }
}