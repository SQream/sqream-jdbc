package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtFloatWriter extends BaseWriter {

    @Override
    public void writeFloat(ByteBuffer buffer, Float value) {
        buffer.putFloat(value);
    }

    @Override
    String getColumnType() {
        return "ftFloat";
    }
}