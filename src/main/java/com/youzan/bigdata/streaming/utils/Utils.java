package com.youzan.bigdata.streaming.utils;

import java.nio.ByteBuffer;

/**
 * Created by chenjunzou on 2017/3/21.
 */
public class Utils {
    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public static byte[] intToBytes(int x) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(x);
        return buffer.array();
    }

    public static byte[] shortToBytes(short x) {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.putShort(x);
        return buffer.array();
    }
}
