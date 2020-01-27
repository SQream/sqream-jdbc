package com.sqream.jdbc.connector.byteReaders;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class FtNvarcharReader extends BaseReader {

    @Override
    public String readNvarchar(ByteBuffer buffer, int nvarcLen, Charset varcharEncoding) {
        byte[] string_bytes = new byte[nvarcLen];

        buffer.get(string_bytes, 0, nvarcLen);

        return new String(string_bytes, 0, nvarcLen, varcharEncoding);
    }

    @Override
    String getColumnType() {
        return "ftVarchar";
    }
}
