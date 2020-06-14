package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtShortWriter extends BaseWriter {

    @Override
    public void writeUbyte(ByteBuffer buffer, Byte value) {
        buffer.putShort(value);
    }

    @Override
    public void writeShort(ByteBuffer buffer, Short value) {
        buffer.putShort(value);
    }

    @Override
    public void writeInt(ByteBuffer buffer, Integer value) {
        buffer.putShort(toShortExact(value));
    }

    @Override
    public void writeLong(ByteBuffer buffer, Long value) {
        buffer.putShort(toShortExact(value));
    }

    @Override
    String getColumnType() {
        return "ftShort";
    }

    private short toShortExact(long value) {
        if ((short)value != value) {
            throw new ArithmeticException("short overflow");
        }
        return (short)value;
    }
}
