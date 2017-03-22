package com.youzan.bigdata.streaming.nsq;

import com.typesafe.scalalogging.slf4j.LazyLogging;
import com.typesafe.scalalogging.slf4j.Logger;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQMessage;
import org.apache.log4j.spi.LoggerFactory;

import java.io.Serializable;


/**
 * Created by chenjunzou on 2017/3/20.
 */
public class BaseMessageHandler implements MessageHandler, Serializable {
    private UnreliableNSQReceiver receiver;
    private int counter = 0;
    public BaseMessageHandler(UnreliableNSQReceiver receiver) {
        this.receiver = receiver;
    }

    public void process(NSQMessage message) {
        counter ++;
        if (counter % 1000 == 0) {
            System.out.println("xxxxxxxx" + message.getReadableContent());
        }
        receiver.store(new NSQMessageWrapper(message));
    }
}
