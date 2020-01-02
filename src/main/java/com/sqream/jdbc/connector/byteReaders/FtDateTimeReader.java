package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtDateTimeReader extends BaseReader {

    @Override
    public long readDateTime(ByteBuffer buffer, int rowIndex) {
        return buffer.getLong(8 * rowIndex);
    }

    @Override
    String getColumnType() {
        return "ftDateTime";
    }
}
