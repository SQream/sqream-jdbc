package com.sqream.jdbc.connector.byteWriters;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class FtVarcharWriter extends BaseWriter {

    @Override
    public int writeVarchar(ByteBuffer buffer, byte[] value, int colLength) {
        // Generate missing spaces to fill up to size
        byte [] spaces = new byte[colLength - value.length];
        Arrays.fill(spaces, (byte) 32);  // ascii value of space
        // Set value and added spaces if needed
        buffer.put(value);
        buffer.put(spaces);
        return colLength;
    }

    @Override
    String getColumnType() {
        return "ftVarchar";
    }

}
