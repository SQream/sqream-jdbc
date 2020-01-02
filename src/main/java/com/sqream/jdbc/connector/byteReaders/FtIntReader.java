package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtIntReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        return read(buffer, rowIndex);
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Short] from column type [ftInt]");
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

    @Override
    public Boolean readBoolean(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Boolean] from column type [ftInt]");
    }

    @Override
    public Byte readUbyte(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Byte] from column type [ftInt]");
    }

    @Override
    public int readDate(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Date] from column type [ftInt]");
    }

    @Override
    public long readDateTime(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [DateTime] from column type [ftInt]");
    }

    private int read(ByteBuffer buffer, int rowIndex) {
        return buffer.getInt(rowIndex * 4);
    }
}
