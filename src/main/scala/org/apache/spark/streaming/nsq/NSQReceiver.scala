package org.apache.spark.streaming.nsq

import com.youzan.nsq.client.entity.NSQMessage
import com.youzan.nsq.client.{Consumer, MessageHandler}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * Created by chenjunzou on 2017/8/15.
  */
abstract class NSQReceiver
  (storageLevel: StorageLevel) extends Receiver[NSQMessage](storageLevel: StorageLevel) {

  def messageHandler: MessageHandler

  def consumer: Consumer
}
