package com.youzan.bigdata.streaming.nsq;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static com.youzan.bigdata.streaming.utils.Utils.intToBytes;
import static com.youzan.bigdata.streaming.utils.Utils.longToBytes;


/**
 * Created by chenjunzou on 2017/3/21.
 */

public class NSQMessageWrapper implements Serializable{
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
        long internalID, traceID, diskQueueOffset;
        int connectionID, diskQueueDataSize, nextConsumingInSecond;
        Address address = new Address("0.0.0.0","0","0", "topic",0);
        timestamp = new byte[s.readInt()];

        s.read(timestamp);
        attempts = new byte[s.readInt()];
        s.read(attempts);
        messageID = new byte[s.readInt()];
        s.read(messageID);
        internalID = s.readLong();
        traceID = s.readLong();
        messageBody = new byte[s.readInt()];
        s.readFully(messageBody);
        address = (Address) s.readObject();
        connectionID = s.readInt();
        diskQueueOffset = s.readLong();
        diskQueueDataSize = s.readInt();
        nextConsumingInSecond = s.readInt();
        message = new NSQMessage(timestamp,attempts, messageID,
                longToBytes(internalID), longToBytes(traceID), longToBytes(diskQueueOffset),
                intToBytes(diskQueueDataSize), messageBody, address, connectionID, nextConsumingInSecond);
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();
        s.writeInt(message.getTimestamp().length);
        s.write(message.getTimestamp());
        s.writeInt(message.getAttempts().length);
        s.write(message.getAttempts());
        s.writeInt(message.getMessageID().length);
        s.write(message.getMessageID());
        s.writeLong(message.getInternalID());
        s.writeLong(message.getTraceID());
        s.writeInt(message.getMessageBody().length);
        s.write(message.getMessageBody());
        s.writeObject(message.getAddress());
        s.writeInt(message.getConnectionID());
        s.writeLong(message.getDiskQueueOffset());
        s.writeInt(message.getDiskQueueDataSize());
        s.writeInt(message.getNextConsumingInSecond());
    }
}
