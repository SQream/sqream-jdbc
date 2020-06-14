package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;
import java.text.MessageFormat;

public class FtUbyteWriter extends BaseWriter {

    @Override
    public void writeUbyte(ByteBuffer buffer, Byte value) {
        if (value < 0) {
            throw new IllegalArgumentException("Trying to set a negative byte value on an unsigned byte column");
        }
        buffer.put(value);
    }

    @Override
    public void writeShort(ByteBuffer buffer, Short value) {
        if (value < 0 || value > 255) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Trying to set wrong value [{0}] on an unsigned byte column", value));
        }
        buffer.put(toShortExact(value).byteValue());
    }

    @Override
    public void writeInt(ByteBuffer buffer, Integer value) {
        buffer.put(toShortExact(value).byteValue());
    }

    @Override
    public void writeLong(ByteBuffer buffer, Long value) {
        buffer.put(toShortExact(value).byteValue());
    }

    @Override
    String getColumnType() {
        return "ftUByte";
    }

    private Short toShortExact(long value) {
        if ((short)value != value) {
            throw new ArithmeticException("byte overflow");
        }
        return (short)value;
    }
}
