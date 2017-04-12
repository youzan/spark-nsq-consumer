package com.youzan.bigdata.streaming.nsq;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;


/**
 * Created by chenjunzou on 2017/3/21.
 */

public class NSQMessageWrapper implements Serializable {
    transient private NSQMessage message;
    public NSQMessageWrapper(NSQMessage message) {
        this.message = message;
    }

    public NSQMessage getMessage() {
        return message;
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException{
        s.defaultReadObject();
        byte[] timestamp, attempts, messageID, messageBody;
        int connectionID;
        Address address = new Address("0.0.0.0","0",null, null, -1);
        timestamp = new byte[s.readInt()];
        s.read(timestamp);
        attempts = new byte[s.readInt()];
        s.read(attempts);
        messageID = new byte[s.readInt()];
        s.read(messageID);
        messageBody = new byte[s.readInt()];
        s.read(messageBody);
        //address = (Address) s.readObject();
        connectionID = s.readInt();
        message = new NSQMessage(timestamp,attempts, messageID, messageBody, address, connectionID);
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();
        s.writeInt(message.getTimestamp().length);
        s.write(message.getTimestamp());
        s.writeInt(message.getAttempts().length);
        s.write(message.getAttempts());
        s.writeInt(message.getMessageID().length);
        s.write(message.getMessageID());
        s.writeInt(message.getMessageBody().length);
        s.write(message.getMessageBody());
        //s.writeObject(message.getAddress());
        s.writeInt(message.getConnectionID());
    }
}
