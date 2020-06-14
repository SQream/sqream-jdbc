package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtDateTimeWriter extends BaseWriter {

    @Override
    public void writeDateTime(ByteBuffer buffer, long value) {
        buffer.putLong(value);
    }

    @Override
    String getColumnType() {
        return "ftDateTime";
    }
}
