package org.apache.spark.streaming.nsq

import java.util.Properties

import com.youzan.nsq.client.{Consumer, ConsumerImplV2}
import com.youzan.nsq.client.entity.NSQConfig
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

/**
  * Created by chenjunzou on 2017/8/15.
  */
abstract class AbstractNSQReceiver
    (nsqParams: Properties,
     storageLevel: StorageLevel)
    extends NSQReceiver(storageLevel)
      with Logging {

  @transient var _consumer: Consumer = _

  def consumer: Consumer =
    _consumer

  def onStop(): Unit = {
    _consumer.close()
  }


  def onStart() {
    logInfo("Starting NSQ Consumer with topic-Channel: (" +
      nsqParams.getProperty("nsq.topic") + ", " + nsqParams.getProperty("nsq.channel") + ")")

    val config = new NSQConfig()
    config.setConsumerName(nsqParams.getProperty("nsq.channel"))
      .setUserSpecifiedLookupAddress(true)
    config.setLookupAddresses(nsqParams.getProperty("nsq.lookup.addresses"))
    config.setConnectTimeoutInMillisecond(nsqParams.getProperty("nsq.connect.timeout.millisecond").toInt)
    config.setMsgTimeoutInMillisecond(nsqParams.getProperty("nsq.msg.timeout.millisecond").toInt)
    config.setThreadPoolSize4IO(nsqParams.getProperty("nsq.threadpool.size4IO").toInt)
    config.setRdy(nsqParams.getProperty("nsq.rdy").toInt)

    if (nsqParams.getProperty("nsq.topic") != null) {
      _consumer = new ConsumerImplV2(config, messageHandler)
      if (!nsqParams.getProperty("nsq.auto.ack").toBoolean) {
        _consumer.setAutoFinish(false)
      }
      _consumer.subscribe(nsqParams.getProperty("nsq.topic"))
      _consumer.start()
    }
  }
}
