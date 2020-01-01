package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtDoubleReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        throw new IllegalArgumentException("Can't retrieve int from column type ftDouble");
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new IllegalArgumentException("Can't retrieve short from column type ftDouble");
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        throw new IllegalArgumentException("Can't retrieve long from column type ftDouble");
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        throw new IllegalArgumentException("Can't retrieve float from column type ftDouble");
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return buffer.getDouble(rowIndex * 8);
    }
}
