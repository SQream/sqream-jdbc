package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;

public class FtDoubleReader implements ByteReader {
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
        return buffer.getDouble(rowIndex * 8);
    }
}
