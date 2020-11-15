package com.sqream.jdbc.connector.byteReaders;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class FtNumericReader extends BaseReader {

    @Override
    public BigDecimal readBigDecimal(ByteBuffer buffer, int rowIndex, int scale) {
        int NUMERIC_SIZE = 16;
        byte[] bytes = readBackward(buffer, rowIndex * NUMERIC_SIZE, NUMERIC_SIZE);
        BigDecimal result = new BigDecimal(new BigInteger(bytes));
        return result.movePointLeft(scale);
    }

    @Override
    String getColumnType() {
        return "ftNumeric";
    }

    private byte[] readBackward(ByteBuffer buffer, int offset, int length) {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[length - i - 1] = buffer.asReadOnlyBuffer().get(offset + i);
        }
        return result;
    }
}
