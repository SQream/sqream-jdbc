package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtDoubleWriter extends BaseWriter {

    @Override
    public int writeDouble(ByteBuffer buffer, Double value, int scale) {
        buffer.putDouble(value);
        return Double.BYTES;
    }

    @Override
    String getColumnType() {
        return "ftDouble";
    }
}
