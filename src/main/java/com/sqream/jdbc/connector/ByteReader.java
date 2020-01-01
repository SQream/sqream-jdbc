package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;

public interface ByteReader {

    Integer readInt(ByteBuffer buffer, int rowIndex);

    Short readShort(ByteBuffer buffer, int rowIndex);

    Long readLong(ByteBuffer buffer, int rowIndex);

    Float readFloat(ByteBuffer buffer, int rowIndex);

    Double readDouble(ByteBuffer buffer, int rowIndex);
}
