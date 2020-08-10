package com.sqream.jdbc.connector.byteReaders;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

import static org.junit.Assert.*;

public class FtLongReaderTest {

    private static final String EXCEPTION_MESSAGE_TEMPLATE = "Trying to get a value of type [%s] from column type [ftLong]";
    private static final int TEST_INT = 123;
    private static final int AMOUNT = 3;
    private static ByteBuffer buffer;
    private static final FtLongReader reader = new FtLongReader();

    @Before
    public void setUp() {
        buffer = ByteBuffer.allocate(30);
    }

    @Test
    public void readLongTest() {
        long[] expected = new long[AMOUNT];
        long value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putLong(value);
            expected[i] = value;
        }

        long result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readLong(buffer, i);
            assertEquals(expected[i], result);
        }
    }

    @Test
    public void readDoubleTest() {
        double[] expected = new double[AMOUNT];
        long value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putLong(value);
            expected[i] = value;
        }

        double result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readDouble(buffer, i);
            assertEquals(expected[i], result, 0);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readBooleanTest() {
        checkExceptionMessage(reader::readBoolean, "Boolean");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readFloatTest() {
        checkExceptionMessage(reader::readFloat, "Float");
    }

    @Test
    public void readIntTest() {
        int[] expected = new int[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putLong(value);
            expected[i] = (int) value;
        }

        long result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readLong(buffer, i);
            assertEquals(expected[i], result);
        }
    }

    @Test
    public void readShortTest() {
        short[] expected = new short[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putLong(value);
            expected[i] = (short) value;
        }

        long result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readLong(buffer, i);
            assertEquals(expected[i], result);
        }
    }

    @Test
    public void readUByteTest() {
        byte[] expected = new byte[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putLong(value);
            expected[i] = (byte) value;
        }

        long result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readLong(buffer, i);
            assertEquals(expected[i], result);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readDateTest() {
        checkExceptionMessage(reader::readDate, "Date");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readDateTimeTest() {
        checkExceptionMessage(reader::readDateTime, "DateTime");
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
