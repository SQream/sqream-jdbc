package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;

public class FtBooleanWriter extends BaseWriter {

    @Override
    public int writeBoolean(ByteBuffer buffer, byte value) {
        buffer.put(value);
        return Byte.BYTES;
    }

    @Override
    String getColumnType() {
        return "ftBoolean";
    }
}
