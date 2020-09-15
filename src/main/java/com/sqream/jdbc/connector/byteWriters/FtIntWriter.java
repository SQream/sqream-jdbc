package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

import static com.sqream.jdbc.utils.Utils.toIntExact;

public class FtIntWriter extends BaseWriter {

    @Override
    public int writeInt(ByteBuffer buffer, Integer value) {
        buffer.putInt(value);
        return Integer.BYTES;
    }

    @Override
    public int writeLong(ByteBuffer buffer, Long value) {
        buffer.putInt(toIntExact(value));
        return Integer.BYTES;
    }

    @Override
    public int writeShort(ByteBuffer buffer, Short value) {
        buffer.putInt(value);
        return Integer.BYTES;
    }

    @Override
    public int writeUbyte(ByteBuffer buffer, Byte value) {
        buffer.putInt(value);
        return Integer.BYTES;
    }

    @Override
    String getColumnType() {
        return "ftInt";
    }
}
