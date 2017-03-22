package com.youzan.bigdata.streaming.nsq

import java.io.Serializable
import java.util.concurrent.{Executor, Executors}

import com.typesafe.scalalogging.slf4j.LazyLogging
import com.youzan.nsq.client.{Consumer, ConsumerImplV2, MessageHandler}
import com.youzan.nsq.client.entity.NSQConfig
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable.Map
import scala.concurrent.Future

/**
  * Created by chenjunzou on 2017/3/20.
  */
class UnreliableNSQReceiver(
        nsqParams: Map[String, String],
        storageLevel: StorageLevel)
  extends Receiver[NSQMessageWrapper](storageLevel)
    with LazyLogging {

  var _consumer: Consumer = _

  def consumer: Option[Consumer] =
    Option(_consumer)

  def onStop(): Unit = {
    if (consumer.isDefined) {
      _consumer.close()
    }
  }

  val messageHandler: MessageHandler = new BaseMessageHandler(this)

  def onStart() {
    logger.info("Starting NSQ Consumer with topic-Channel: (" +
      nsqParams("nsq.topic") + ", " + nsqParams("nsq.channel") + ")")

    val config = new NSQConfig()
    config.setConsumerName(nsqParams("nsq.channel").toString)
      .setUserSpecifiedLookupAddress(true)
    config.setLookupAddresses(nsqParams("nsq.lookup.addresses").toString)
    config.setConnectTimeoutInMillisecond(nsqParams("nsq.connect.timeout.millisecond").toString.toInt)
    config.setMsgTimeoutInMillisecond(nsqParams("nsq.msg.timeout.millisecond").toString.toInt)
    config.setThreadPoolSize4IO(nsqParams("nsq.threadpool.size4IO").toString.toInt)

    if (nsqParams("nsq.topic") != null) {
      _consumer = new ConsumerImplV2(config, messageHandler)
      _consumer.subscribe(nsqParams("nsq.topic"))
      _consumer.start()
    }
  }

}
