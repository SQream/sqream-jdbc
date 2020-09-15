package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtLongWriter extends BaseWriter {

    @Override
    public int writeLong(ByteBuffer buffer, Long value) {
        buffer.putLong(value);
        return Long.BYTES;
    }

    @Override
    public int writeInt(ByteBuffer buffer, Integer value) {
        buffer.putLong(value);
        return Long.BYTES;
    }

    @Override
    public int writeShort(ByteBuffer buffer, Short value) {
        buffer.putLong(value);
        return Long.BYTES;
    }

    @Override
    public int writeUbyte(ByteBuffer buffer, Byte value) {
        buffer.putLong(value);
        return Long.BYTES;
    }

    @Override
    String getColumnType() {
        return "ftLong";
    }
}
