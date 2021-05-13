package com.sqream.jdbc.connector.byteWriters;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public interface ByteWriter {

    int writeInt(ByteBuffer buffer, Integer value);

    int writeShort(ByteBuffer buffer, Short value);

    int writeLong(ByteBuffer buffer, Long value);

    int writeFloat(ByteBuffer buffer, Float value, int scale);

    int writeDouble(ByteBuffer buffer, Double value, int scale);

    int writeNumeric(ByteBuffer buffer, BigDecimal value, int scale);

    int writeBoolean(ByteBuffer buffer, byte value);

    int writeUbyte(ByteBuffer buffer, Byte value);

    int writeDate(ByteBuffer buffer, int value);

    int writeDateTime(ByteBuffer buffer, long value);

    int writeVarchar(ByteBuffer buffer, byte[] value, int colLength);

    int writeNvarchar(ByteBuffer buffer, byte[] value);
}
