package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

import static com.sqream.jdbc.utils.Utils.*;

public class FtLongReader extends BaseReader {

    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        return toIntExact(read(buffer, rowIndex));
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        return toShortExact(read(buffer, rowIndex));
    }

    @Override
    public Byte readUbyte(ByteBuffer buffer, int rowIndex) {
        return toByteExact(read(buffer, rowIndex));
    }

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
