package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtBooleanReader implements ByteReader {
    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Integer] from column type [ftBoolean]");
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Short] from column type [ftBoolean]");
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Long] from column type [ftBoolean]");
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Float] from column type [ftBoolean]");
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Double] from column type [ftBoolean]");
    }

    @Override
    public Boolean readBoolean(ByteBuffer buffer, int rowIndex) {
        return buffer.get(rowIndex) != 0;
    }

    @Override
    public Byte readUbyte(ByteBuffer buffer, int rowIndex) {
        throw new UnsupportedOperationException("Trying to get a value of type [Byte] from column type [ftBoolean]");
    }
}
