package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtFloatReader implements ByteReader {

    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        return buffer.getFloat(rowIndex * 4);
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return (double) buffer.getFloat(rowIndex * 4);
    }
}
