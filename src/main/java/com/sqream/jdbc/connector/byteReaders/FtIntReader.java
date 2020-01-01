package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtIntReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        return buffer.getInt(rowIndex * 4);
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new IllegalArgumentException("Can't retrieve short from column type ftInt");
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        return (long) buffer.getInt(rowIndex * 4);
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        return (float) buffer.getInt(rowIndex * 4);
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return (double) buffer.getInt(rowIndex * 4);
    }
}
