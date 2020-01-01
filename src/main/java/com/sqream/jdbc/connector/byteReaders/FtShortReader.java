package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtShortReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        return (int) read(buffer, rowIndex);
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        return read(buffer, rowIndex);
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        return (long) read(buffer, rowIndex);
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        return (float) read(buffer, rowIndex);
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return (double) read(buffer, rowIndex);
    }

    private short read(ByteBuffer buffer, int rowIndex) {
        return buffer.getShort(rowIndex * 2);
    }
}
