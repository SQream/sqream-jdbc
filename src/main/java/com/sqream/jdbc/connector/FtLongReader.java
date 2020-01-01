package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;

public class FtLongReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        throw new IllegalArgumentException("Can't retrieve int from column type ftLong");
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new IllegalArgumentException("Can't retrieve short from column type ftLong");
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        return buffer.getLong(rowIndex * 8);
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return (double) buffer.getLong(rowIndex * 8);
    }
}
