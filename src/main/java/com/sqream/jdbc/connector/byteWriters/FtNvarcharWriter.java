package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtNvarcharWriter extends BaseWriter {

    @Override
    public void writeNvarchar(ByteBuffer buffer, byte[] value) {
        buffer.put(value);
    }

    @Override
    String getColumnType() {
        return "ftNvarchar";
    }

}
