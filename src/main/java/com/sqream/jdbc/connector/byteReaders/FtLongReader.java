package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtLongReader extends BaseReader {

    @Override
    public Long readLong(ByteBuffer buffer, int rowIndex) {
        return read(buffer, rowIndex);
    }

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return (double) read(buffer, rowIndex);
    }

    @Override
    String getColumnType() {
        return "ftLong";
    }

    private long read(ByteBuffer buffer, int rowIndex) {
        return buffer.getLong(rowIndex * 8);
    }
}
