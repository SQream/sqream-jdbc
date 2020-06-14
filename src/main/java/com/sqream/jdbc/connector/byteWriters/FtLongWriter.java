package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtLongWriter extends BaseWriter {

    @Override
    public void writeLong(ByteBuffer buffer, Long value) {
        buffer.putLong(value);
    }

    @Override
    public void writeInt(ByteBuffer buffer, Integer value) {
        buffer.putLong(value);
    }

    @Override
    public void writeShort(ByteBuffer buffer, Short value) {
        buffer.putLong(value);
    }

    @Override
    public void writeUbyte(ByteBuffer buffer, Byte value) {
        buffer.putLong(value);
    }

    @Override
    String getColumnType() {
        return "ftLong";
    }
}
