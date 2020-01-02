package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtLongReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Integer] from column type [ftLong]");
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Short] from column type [ftLong]");
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        return read(buffer, rowIndex);
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Float] from column type [ftLong]");
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return (double) read(buffer, rowIndex);
    }

    @Override
    public Boolean readBoolean(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Boolean] from column type [ftLong]");
    }

    @Override
    public Byte readUbyte(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Byte] from column type [ftLong]");
    }

    private long read(ByteBuffer buffer, int rowIndex) {
        return buffer.getLong(rowIndex * 8);
    }
}
