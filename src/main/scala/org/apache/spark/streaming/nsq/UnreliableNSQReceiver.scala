package org.apache.spark.streaming.nsq

import com.youzan.bigdata.streaming.nsq.BaseMessageHandler
import com.youzan.nsq.client.MessageHandler
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel


/**
  * Created by chenjunzou on 2017/3/20.
  */
class UnreliableNSQReceiver(
        nsqParams: Map[String, String],
        storageLevel: StorageLevel)
  extends AbstractNSQReceiver(nsqParams, storageLevel)
    with Logging {

  override val messageHandler: MessageHandler = new BaseMessageHandler(this)
}
