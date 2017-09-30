package org.apache.spark.streaming.nsq

import java.util.Properties

import com.youzan.bigdata.streaming.nsq.{BaseMessageHandler, NSQMessageWrapper}
import com.youzan.nsq.client.MessageHandler
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener}

import scala.collection.mutable

/**
  * Created by chenjunzou on 2017/3/20.
  */
class ReliableNSQReceiver(
        nsqParams: Properties,
        storageLevel: StorageLevel)
  extends AbstractNSQReceiver(nsqParams, storageLevel)
    with Logging {

  private var _blockGenerator: BlockGenerator = null

  val messageHandler: MessageHandler = new BaseMessageHandler(this)

  def blockGenerator: BlockGenerator = _blockGenerator



  override def onStart(): Unit = {
    if (nsqParams.getProperty("nsq.auto.ack").toBoolean) {
      logWarning("nsq.auto.ack must be set to false in reliable mode, now turn it to false")
      nsqParams.setProperty("nsq.auto.ack", "false")
    }
    _blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)
    _blockGenerator.start()

    super.onStart()
  }

  /**
    * Store the ready-to-be-stored block . This method
    * will try a fixed number of times to push the block. If the push fails, the receiver is stopped.
    */
  private def storeBlockAndAck(
        blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
    var count = 0
    var pushed = false
    var exception: Exception = null
    while (!pushed && count <= 3) {
      try {
        store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[NSQMessageWrapper]])
        pushed = true
      } catch {
        case ex: Exception =>
          count += 1
          exception = ex
      }
    }
    if (pushed) {
      logInfo("block " + blockId + " pushed " + arrayBuffer.length + "messages")
      for (msg <- arrayBuffer) {
        consumer.finish(msg.asInstanceOf[NSQMessageWrapper].getMessage)
      }
    } else {
      stop("Error while storing block into Spark", exception)
    }
  }

  /** Class to handle blocks generated by the block generator. */
  private final class GeneratedBlockHandler extends BlockGeneratorListener {

    def onAddData(data: Any, metadata: Any): Unit = {
//      consumer.finish(data.asInstanceOf[NSQMessageWrapper].getMessage)
    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      // Store block and commit the blocks offset
      storeBlockAndAck(blockId, arrayBuffer)
    }

    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }
}
