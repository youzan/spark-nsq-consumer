package com.youzan.bigdata.streaming.example

import java.io.FileInputStream
import java.util.Properties

import com.youzan.bigdata.streaming.cache.RedisConnPool
import com.youzan.bigdata.streaming.nsq.NsqInputDStream
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  * Created by chenjunzou on 2017/3/23.
  */
object AgentForwardExample {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("NSQ test")
      .setIfMissing("spark.master", "local[2]") //for console debug

    val config = args(0)
    val prop = new Properties()
    prop.load(new FileInputStream(config))

    //val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(5))

    val redisPool = new RedisConnPool(prop)
    redisPool.init()
    val redisBroadcast = ssc.sparkContext.broadcast(redisPool)

    val source = new NsqInputDStream(ssc, prop)
    source.map(wraper => wraper.getMessage.getReadableContent)
          .map(line => {
            val splits = line.split("\t")
            (splits(3), 1)
          }).reduceByKey(_+_)
          .foreachRDD(rdd =>
            {
              rdd.foreachPartition( par => {
                val redisPool = redisBroadcast.value
                val jedis = redisPool.getConnection
                  for ((host: String, count: Int) <- par) {
                    if (jedis.get(host) != null) {
                      val originCount = jedis.get(host).toInt
                      jedis.set(host, (originCount + count) toString)
                    } else {
                      jedis.set(host, count.toString)
                    }
                    println(host + ":" + jedis.get(host))
                  }
                redisPool.returnResource(jedis)
              })
            })

    ssc.start()
    ssc.awaitTermination()
  }
}
