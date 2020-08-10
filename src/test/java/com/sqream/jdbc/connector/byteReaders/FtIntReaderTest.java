package com.sqream.jdbc.connector.byteReaders;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

import static org.junit.Assert.*;

public class FtIntReaderTest {

    private static final String EXCEPTION_MESSAGE_TEMPLATE = "Trying to get a value of type [%s] from column type [ftInt]";
    private static final int TEST_INT = 123;
    private static final int AMOUNT = 3;
    private static ByteBuffer buffer;
    private static final FtIntReader reader = new FtIntReader();

    @Before
    public void setUp() {
        buffer = ByteBuffer.allocate(30);
    }

    @Test
    public void readDoubleTest() {
        double[] expected = new double[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putInt(value);
            expected[i] = value;
        }

        double result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readDouble(buffer, i);
            assertEquals(expected[i], result, 0);
        }
    }

    @Test
    public void readIntTest() {
        int[] expected = new int[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putInt(value);
            expected[i] = value;
        }

        double result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readInt(buffer, i);
            assertEquals(expected[i], result, 0);
        }
    }

    @Test
    public void readLongTest() {
        long[] expected = new long[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putInt(value);
            expected[i] = value;
        }

        double result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readLong(buffer, i);
            assertEquals(expected[i], result, 0);
        }
    }

    @Test
    public void readFloatTest() {
        float[] expected = new float[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putInt(value);
            expected[i] = value;
        }

        double result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readFloat(buffer, i);
            assertEquals(expected[i], result, 0);
        }
    }

    @Test
    public void readUByteTest() {
        byte[] expected = new byte[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putInt(value);
            expected[i] = (byte)value;
        }

        double result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readUbyte(buffer, i);
            assertEquals(expected[i], result, 0);
        }
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

    @Test
    public void readShortTest() {
        short[] expected = new short[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putInt(value);
            expected[i] = (short) value;
        }

        double result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readShort(buffer, i);
            assertEquals(expected[i], result, 0);
        }
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
