package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtFloatWriter extends BaseWriter {

    @Override
    public int writeFloat(ByteBuffer buffer, Float value, int scale) {
        buffer.putFloat(value);
        return Float.BYTES;
    }

    @Override
    String getColumnType() {
        return "ftFloat";
    }
}
