package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtFloatReader extends BaseReader {

    @Override
    public Float readFloat(ByteBuffer buffer, int rowIndex) {
        return read(buffer, rowIndex);
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return (double) read(buffer, rowIndex);
    }

    @Override
    String getColumnType() {
        return "ftFloat";
    }

    public float read(ByteBuffer buffer, int rowIndex) {
        return buffer.getFloat(rowIndex * 4);
    }
}
