package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtDateReader extends BaseReader {

    @Override
    public int readDate(ByteBuffer buffer, int rowIndex) {
        return buffer.getInt(4* rowIndex);
    }

    @Override
    String getColumnType() {
        return "ftDate";
    }
}
