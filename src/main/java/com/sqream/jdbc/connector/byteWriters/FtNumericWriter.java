package com.sqream.jdbc.connector.byteWriters;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

public class FtNumericWriter extends BaseWriter {

    @Override
    public int writeNumeric(ByteBuffer buffer, BigDecimal value, int scale) {
        int NUMERIC_SIZE = 16;
        byte padding = (byte) value.signum() == -1 ? (byte) -1 : (byte) 0;
        BigDecimal rounded = value.setScale(scale, RoundingMode.CEILING);
        byte[] bytes = rounded.unscaledValue().toByteArray();
        for (int i = 0; i < NUMERIC_SIZE; i++) {
            if (i < bytes.length) {
                buffer.put(bytes[bytes.length - i - 1]);
            } else {
                buffer.put(padding);
            }
        }
        return NUMERIC_SIZE;
    }

    @Override
    String getColumnType() {
        return "ftNumeric";
    }
}
