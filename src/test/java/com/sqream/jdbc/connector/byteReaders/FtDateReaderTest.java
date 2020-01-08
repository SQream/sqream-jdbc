package com.sqream.jdbc.connector.byteReaders;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

import static org.junit.Assert.*;

public class FtDateReaderTest {
    private static final String EXCEPTION_MESSAGE_TEMPLATE = "Trying to get a value of type [%s] from column type [ftDate]";
    private static final int AMOUNT = 3;
    private static final int TEST_INT = 123;
    private static ByteBuffer buffer;
    private static final FtDateReader reader = new FtDateReader();

    @Before
    public void setUp() {
        buffer = ByteBuffer.allocate(30);
    }

    @Test
    public void readDateTest() {
        int[] expected = new int[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putInt(value);
            expected[i] = value;
        }

        double result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readDate(buffer, i);
            assertEquals(expected[i], result, 0);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readDatetimeTest() {
        checkExceptionMessage(reader::readDateTime, "DateTime");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readShortTest() {
        checkExceptionMessage(reader::readShort, "Short");
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
    public void readFloatTest() {
        checkExceptionMessage(reader::readFloat, "Float");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readIntegerTest() {
        checkExceptionMessage(reader::readInt, "Integer");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readDoubleTest() {
        checkExceptionMessage(reader::readDouble, "Double");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readLongTest() {
        checkExceptionMessage(reader::readLong, "Long");
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