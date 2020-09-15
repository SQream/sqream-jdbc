package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtDateTimeWriter extends BaseWriter {

    @Override
    public int writeDateTime(ByteBuffer buffer, long value) {
        buffer.putLong(value);
        return Long.BYTES;
    }

    @Override
    String getColumnType() {
        return "ftDateTime";
    }
}
