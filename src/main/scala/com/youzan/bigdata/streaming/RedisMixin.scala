package com.youzan.bigdata.streaming

import com.youzan.bigdata.streaming.cache.RedisConnPool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * Created by chenjunzou on 2017/3/24.
  */
trait RedisMixin[V]
{
  def getRedisPool(): RedisConnPool

  def broadcastRedisPool(): Broadcast[RedisConnPool]

  def execute: ((V, Jedis) => Unit)

  def RedisProcedure(dStream: DStream[V]): Unit = {
    dStream.foreachRDD(rdd => {
      rdd.foreachPartition(
        par => {
          val redisPool = broadcastRedisPool.value
          val jedis = redisPool.getConnection
          for (it <- par) {
            execute(it, jedis)
          }
          redisPool.returnResource(jedis)
        }
      )
    })
  }
}
