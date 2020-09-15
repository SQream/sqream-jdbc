package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtDateWriter extends BaseWriter {

    @Override
    public int writeDate(ByteBuffer buffer, int value) {
        buffer.putInt(value);
        return Integer.BYTES;
    }

    @Override
    String getColumnType() {
        return "ftDate";
    }
}
