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
@Deprecated
public class NSQMessageWrapper implements Serializable{
    transient private NSQMessage message;
    public NSQMessageWrapper(NSQMessage message) {
        this.message = message;
    }

    public NSQMessage getMessage() {
        return message;
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException{
        throw new IOException("the wrapper class is deprecated because NSQMessage is serializable now and " +
            "its constructor is changed often, the wrapper class is not needed any more");
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
        throw new IOException("the wrapper class is deprecated because NSQMessage is serializable now and " +
            "its constructor is changed often, the wrapper class is not needed any more");
    }
}
