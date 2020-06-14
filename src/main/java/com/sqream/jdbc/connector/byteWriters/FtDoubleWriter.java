package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtDoubleWriter extends BaseWriter {

    @Override
    public void writeDouble(ByteBuffer buffer, Double value) {
        buffer.putDouble(value);
    }

    @Override
    String getColumnType() {
        return "ftDouble";
    }
}
