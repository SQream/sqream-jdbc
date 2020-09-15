package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtNvarcharWriter extends BaseWriter {

    @Override
    public int writeNvarchar(ByteBuffer buffer, byte[] value) {
        buffer.put(value);
        return value.length;
    }

    @Override
    String getColumnType() {
        return "ftNvarchar";
    }

}
