package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtDateReader implements ByteReader {

    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        return null;
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        return null;
    }

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        return null;
    }

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        return null;
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return null;
    }

    @Override
    public Boolean readBoolean(ByteBuffer buffer, int rowIndex) {
        return null;
    }

    @Override
    public Byte readUbyte(ByteBuffer buffer, int rowIndex) {
        return null;
    }

    @Override
    public int readDate(ByteBuffer buffer, int rowIndex) {
        return buffer.getInt(4* rowIndex);
    }
}
