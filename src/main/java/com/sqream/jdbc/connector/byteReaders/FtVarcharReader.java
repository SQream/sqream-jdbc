package com.sqream.jdbc.connector.byteReaders;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class FtVarcharReader extends BaseReader {

    @Override
    public String readVarchar(ByteBuffer buffer, int colSize, String varcharEncoding) {
        byte[] string_bytes = new byte[colSize];

        buffer.get(string_bytes, 0, colSize);

        try {
            return ("X" + (new String(string_bytes, 0, colSize, varcharEncoding))).trim().substring(1);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Exception when trying to encode varchar value", e);
        }
    }

    @Override
    String getColumnType() {
        return "ftVarchar";
    }
}
