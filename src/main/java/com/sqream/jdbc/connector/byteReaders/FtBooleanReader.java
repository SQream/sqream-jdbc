package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtBooleanReader extends BaseReader {

    @Override
    public Boolean readBoolean(ByteBuffer buffer, int rowIndex) {
        return buffer.get(rowIndex) != 0;
    }

    @Override
    String getColumnType() {
        return "ftBool";
    }
}
