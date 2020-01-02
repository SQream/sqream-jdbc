package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;
import java.sql.Date;

public interface ByteReader {

    Integer readInt(ByteBuffer buffer, int rowIndex);

    Short readShort(ByteBuffer buffer, int rowIndex);

    Long readLong(ByteBuffer buffer, int rowIndex);

    Float readFloat(ByteBuffer buffer, int rowIndex);

    Double readDouble(ByteBuffer buffer, int rowIndex);

    Boolean readBoolean(ByteBuffer buffer, int rowIndex);

    Byte readUbyte(ByteBuffer buffer, int rowIndex);

    int readDate(ByteBuffer buffer, int rowIndex);

    long readDateTime(ByteBuffer buffer, int rowIndex);
}
