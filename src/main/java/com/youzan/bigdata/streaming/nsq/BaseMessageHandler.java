package com.youzan.bigdata.streaming.nsq;

import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQMessage;
import org.apache.spark.streaming.nsq.NSQReceiver;
import org.apache.spark.streaming.nsq.ReliableNSQReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


/**
 * Created by chenjunzou on 2017/3/20.
 */
public class BaseMessageHandler implements MessageHandler, Serializable{
    private static final Logger logger = LoggerFactory.getLogger(BaseMessageHandler.class);

    private NSQReceiver receiver;
    private int counter = 0;
    public BaseMessageHandler(NSQReceiver receiver) {
        this.receiver = receiver;
    }

    public void process(NSQMessage message) {
        counter ++;
        logger.debug("adding message:" + message.getMessageID());
        if (receiver instanceof ReliableNSQReceiver) {
            ((ReliableNSQReceiver) receiver).blockGenerator().addData(new NSQMessageWrapper(message));
        } else {
            receiver.store(new NSQMessageWrapper(message));
        }
    }
}
