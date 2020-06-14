package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public interface ByteWriter {

    void writeInt(ByteBuffer buffer, Integer value);

    void writeShort(ByteBuffer buffer, Short value);

    void writeLong(ByteBuffer buffer, Long value);

    void writeFloat(ByteBuffer buffer, Float value);

    void writeDouble(ByteBuffer buffer, Double value);

    void writeBoolean(ByteBuffer buffer, byte value);

    void writeUbyte(ByteBuffer buffer, Byte value);

    void writeDate(ByteBuffer buffer, int value);

    void writeDateTime(ByteBuffer buffer, long value);

    void writeVarchar(ByteBuffer buffer, byte[] value, int colLength);

    void writeNvarchar(ByteBuffer buffer, byte[] value);
}
