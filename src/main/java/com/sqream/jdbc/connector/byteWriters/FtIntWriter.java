package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtIntWriter extends BaseWriter {

    @Override
    public void writeInt(ByteBuffer buffer, Integer value) {
        buffer.putInt(value);
    }

    @Override
    public void writeLong(ByteBuffer buffer, Long value) {
        buffer.putInt(toIntExact(value));
    }

    @Override
    public void writeShort(ByteBuffer buffer, Short value) {
        buffer.putInt(value);
    }

    @Override
    public void writeUbyte(ByteBuffer buffer, Byte value) {
        buffer.putInt(value);
    }

    @Override
    String getColumnType() {
        return "ftInt";
    }

    private int toIntExact(long value) {
        if ((int)value != value) {
            throw new ArithmeticException("int overflow");
        }
        return (int)value;
    }
}
