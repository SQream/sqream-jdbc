package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;

public class FtShortReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        return (int) buffer.getShort(rowIndex * 2);
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        return buffer.getShort(rowIndex * 2);
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        return (long) buffer.getShort(rowIndex * 2);
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        return (float) buffer.getShort(rowIndex * 2);
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return (double) buffer.getShort(rowIndex * 2);
    }
}
