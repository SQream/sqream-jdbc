package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtBooleanWriter extends BaseWriter {

    @Override
    public void writeBoolean(ByteBuffer buffer, byte value) {
        buffer.put(value);
    }

    @Override
    String getColumnType() {
        return "ftBoolean";
    }
}
