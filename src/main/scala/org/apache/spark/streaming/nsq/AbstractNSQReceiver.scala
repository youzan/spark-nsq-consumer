package org.apache.spark.streaming.nsq

import com.youzan.nsq.client.{Consumer, ConsumerImplV2}
import com.youzan.nsq.client.entity.NSQConfig
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

/**
  * Created by chenjunzou on 2017/8/15.
  */
abstract class AbstractNSQReceiver
    (nsqParams: Map[String, String],
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
      nsqParams("nsq.topic") + ", " + nsqParams("nsq.channel") + ")")

    val config = new NSQConfig()
    config.setConsumerName(nsqParams("nsq.channel").toString)
      .setUserSpecifiedLookupAddress(true)
    config.setLookupAddresses(nsqParams("nsq.lookup.addresses").toString)
    config.setConnectTimeoutInMillisecond(nsqParams("nsq.connect.timeout.millisecond").toString.toInt)
    config.setMsgTimeoutInMillisecond(nsqParams("nsq.msg.timeout.millisecond").toString.toInt)
    config.setThreadPoolSize4IO(nsqParams("nsq.threadpool.size4IO").toString.toInt)
    config.setRdy(nsqParams("nsq.rdy").toString.toInt)

    if (nsqParams("nsq.topic") != null) {
      _consumer = new ConsumerImplV2(config, messageHandler)
      if (!nsqParams.getOrElse("nsq.auto.ack", "true").toBoolean) {
        _consumer.setAutoFinish(false);
      }
      _consumer.subscribe(nsqParams("nsq.topic"))
      _consumer.start()
    }
  }
}
