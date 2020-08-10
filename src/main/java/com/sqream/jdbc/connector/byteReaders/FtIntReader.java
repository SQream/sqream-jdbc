package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

import static com.sqream.jdbc.utils.Utils.toShortExact;
import static com.sqream.jdbc.utils.Utils.toByteExact;

public class FtIntReader extends BaseReader {
    @Override
    public Byte readUbyte(ByteBuffer buffer, int rowIndex) {
        return toByteExact(read(buffer, rowIndex));
    }

    @Override
    public Short readShort(ByteBuffer buffer, int rowIndex) {
        return toShortExact(read(buffer, rowIndex));
    }

    @Override
    public Integer readInt(ByteBuffer buffer, int rowIndex) {
        return read(buffer, rowIndex);
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
    String getColumnType() {
        return "ftInt";
    }

    private int read(ByteBuffer buffer, int rowIndex) {
        return buffer.getInt(rowIndex * 4);
    }
}
