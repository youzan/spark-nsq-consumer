package com.youzan.bigdata.streaming

import com.youzan.bigdata.streaming.nsq.NSQMessageWrapper
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by chenjunzou on 2017/3/24.
  */
abstract class StreamingBase[T: ClassTag, V: ClassTag] {
  var streamingContext: StreamingContext = _

  def extract: (T => V)
  def getContext: SparkContext = {
    streamingContext.sparkContext
  }
  def init(context: StreamingContext): Unit = {
    streamingContext = context
  }

  def getSourceDStream: DStream[T]

  def run(): Unit

}
