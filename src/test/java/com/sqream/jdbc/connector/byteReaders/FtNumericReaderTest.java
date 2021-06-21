package com.sqream.jdbc.connector.byteReaders;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FtNumericReaderTest {

    private static final String EXCEPTION_MESSAGE_TEMPLATE = "Trying to get a value of type [%s] from column type [ftNumeric]";
    private static final byte TEST_INT = 123;
    private static final int AMOUNT = 3;
    private static final int NUMERIC_SIZE = 16;
    private static ByteBuffer buffer;
    private static final FtNumericReader reader = new FtNumericReader();

    @Before
    public void setUp() {
        buffer = ByteBuffer.allocate(AMOUNT * NUMERIC_SIZE);
    }

    @Test
    public void readNumericTest() {
        double[] expectedDouble = new double[AMOUNT];
        BigDecimal value;
        for (int i = 0; i < AMOUNT; i++) {
            value = new BigDecimal(TEST_INT + i);
            buffer.put((byte)(TEST_INT + i));
            for (int j = 0; j < NUMERIC_SIZE - 1; j++) {
                buffer.put((byte)0);
            }
            expectedDouble[i] = value.doubleValue();
        }

        double result;
        for (int i = 0; i < expectedDouble.length; i++) {
            result = reader.readDouble(buffer, i, 0);
            assertEquals(expectedDouble[i], result, 0);
        }

        BigDecimal result2;
        for (int i = 0; i < expectedDouble.length; i++) {
            result2 = reader.readBigDecimal(buffer, i, 0);
            assertEquals(expectedDouble[i], result2.doubleValue(), 0);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readLongTest() {
        checkExceptionMessage(reader::readLong, "Long");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readFloatTest() {
        checkExceptionMessage(reader::readFloat, "Float");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readIntTest() {
        checkExceptionMessage(reader::readInt, "Integer");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readUByteTest() {
        checkExceptionMessage(reader::readUbyte, "Ubyte");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readBooleanTest() {
        checkExceptionMessage(reader::readBoolean, "Boolean");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readDateTest() {
        checkExceptionMessage(reader::readDate, "Date");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readDatetimeTest() {
        checkExceptionMessage(reader::readDateTime, "DateTime");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readShortTest() {
        checkExceptionMessage(reader::readShort, "Short");
    }

    private void checkExceptionMessage(BiFunction<ByteBuffer, Integer, Object> function, String valueType) {
        try {
            function.apply(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (String.format(EXCEPTION_MESSAGE_TEMPLATE, valueType).equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }
}
