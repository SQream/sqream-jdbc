package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtShortWriter extends BaseWriter {

    @Override
    public int writeUbyte(ByteBuffer buffer, Byte value) {
        buffer.putShort(value);
        return Short.BYTES;
    }

    @Override
    public int writeShort(ByteBuffer buffer, Short value) {
        buffer.putShort(value);
        return Short.BYTES;
    }

    @Override
    public int writeInt(ByteBuffer buffer, Integer value) {
        buffer.putShort(toShortExact(value));
        return Short.BYTES;
    }

    @Override
    public int writeLong(ByteBuffer buffer, Long value) {
        buffer.putShort(toShortExact(value));
        return Short.BYTES;
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
