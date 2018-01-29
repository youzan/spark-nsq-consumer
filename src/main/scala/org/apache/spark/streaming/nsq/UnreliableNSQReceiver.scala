package org.apache.spark.streaming.nsq

import java.util.Properties

import com.youzan.bigdata.streaming.nsq.BaseMessageHandler
import com.youzan.nsq.client.MessageHandler
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel


/**
  * Created by chenjunzou on 2017/3/20.
  */
class UnreliableNSQReceiver(
        nsqParams: Properties,
        storageLevel: StorageLevel)
  extends AbstractNSQReceiver(nsqParams, storageLevel)
    with Logging {

  override val messageHandler: MessageHandler = new BaseMessageHandler(this)

  override def onStart(): Unit = {
    if (!nsqParams.getProperty("nsq.auto.ack").toBoolean) {
      logWarning("nsq.auto.ack must be set to true in unreliable mode, otherwise ack will not send to the server, " +
        "leading messages to send again, now turn it to true")
      nsqParams.setProperty("nsq.auto.ack", "true")
    }
    super.onStart()
  }
}
