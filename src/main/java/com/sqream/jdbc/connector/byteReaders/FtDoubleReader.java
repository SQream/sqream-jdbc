package com.sqream.jdbc.connector.byteReaders;

import java.nio.ByteBuffer;

public class FtDoubleReader extends BaseReader {

    @Override
    public Double readDouble(ByteBuffer buffer, int rowIndex) {
        return buffer.getDouble(rowIndex * 8);
    }

    @Override
    String getColumnType() {
        return "ftDouble";
    }
}
