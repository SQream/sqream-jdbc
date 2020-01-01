package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtUbyteReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        return buffer.get(rowIndex) & 0xFF;
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        return (short) (buffer.get(rowIndex) & 0xFF);
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        return (long) buffer.get(rowIndex) & 0xFF;
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        return (float) (buffer.get(rowIndex) & 0xFF);
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return (double) (buffer.get(rowIndex) & 0xFF);
    }
}
