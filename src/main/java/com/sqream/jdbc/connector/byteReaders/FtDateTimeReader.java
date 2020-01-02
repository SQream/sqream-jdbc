package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtDateTime implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Integer] from column type [ftDateTime]");
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Short] from column type [ftDateTime]");
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Long] from column type [ftDateTime]");
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Float] from column type [ftDateTime]");
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [DateTime] from column type [ftDateTime]");
    }

    @Override
    public Boolean readBoolean(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Boolean] from column type [ftDateTime]");
    }

    @Override
    public Byte readUbyte(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [DateTime] from column type [ftDateTime]");
    }

    @Override
    public int readDate(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Date] from column type [ftDateTime]");
    }

    @Override
    public long readDateTime(ByteBuffer buffer, int rowIndex) {
        return buffer.getLong(8 * rowIndex);
    }
}
