package com.sqream.jdbc.connector.byteReaders;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FtBooleanReaderTest {

    private static final String EXCEPTION_MESSAGE_TEMPLATE = "Trying to get a value of type [%s] from column type [ftBool]";
    private static final int AMOUNT = 3;
    private static ByteBuffer buffer;
    private static final FtBooleanReader reader = new FtBooleanReader();

    @Before
    public void setUp() {
        buffer = ByteBuffer.allocate(30);
    }

    @Test
    public void readBooleanTest() {
        boolean[] expected = new boolean[AMOUNT];
        byte value;
        boolean booleanValue;
        for (int i = 0; i < AMOUNT; i++) {
            booleanValue = i % 2 == 0;
            value = (byte) (booleanValue ? 1 : 0);
            buffer.put(value);
            expected[i] = booleanValue;
        }

        boolean result;
        for (int i = 0; i < expected.length; i++) {
            result = reader.readBoolean(buffer, i);
            assertEquals(expected[i], result);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readShortTest() {
        checkExceptionMessage(reader::readShort, "Short");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readIntTest() {
        checkExceptionMessage(reader::readInt, "Integer");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readDateTest() {
        checkExceptionMessage(reader::readDate, "Date");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readDateTimeTest() {
        checkExceptionMessage(reader::readDateTime, "DateTime");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readUbyteTest() {
        checkExceptionMessage(reader::readUbyte, "Ubyte");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readFloatTest() {
        checkExceptionMessage(reader::readFloat, "Float");
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