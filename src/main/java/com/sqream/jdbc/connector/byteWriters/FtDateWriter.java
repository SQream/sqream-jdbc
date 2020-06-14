package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtDateWriter extends BaseWriter {

    @Override
    public void writeDate(ByteBuffer buffer, int value) {
        buffer.putInt(value);
    }

    @Override
    String getColumnType() {
        return "ftDate";
    }
}
