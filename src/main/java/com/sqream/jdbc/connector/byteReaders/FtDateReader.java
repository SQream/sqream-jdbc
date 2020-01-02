package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtDateReader implements ByteReader {

    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Integer] from column type [ftDate]");
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Short] from column type [ftDate]");
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Long] from column type [ftDate]");
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Float] from column type [ftDate]");
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Double] from column type [ftDate]");
    }

    @Override
    public Boolean readBoolean(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Boolean] from column type [ftDate]");
    }

    @Override
    public Byte readUbyte(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Ubyte] from column type [ftDate]");
    }

    @Override
    public int readDate(ByteBuffer buffer, int rowIndex) {
        return buffer.getInt(4* rowIndex);
    }

    @Override
    public long readDateTime(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [DateTime] from column type [ftDate]");
    }
}
