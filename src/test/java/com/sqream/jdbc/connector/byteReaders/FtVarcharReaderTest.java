package com.sqream.jdbc.connector.byteReaders;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;

import static org.junit.Assert.*;

public class FtVarcharReaderTest {

    private static final String EXCEPTION_MESSAGE_TEMPLATE = "Trying to get a value of type [%s] from column type [ftVarchar]";
    private static final String DEFAULT_CHARACTER_CODES = "ascii";
    private static final String TEST_STRING = "Test value";
    private static final int AMOUNT = 3;
    private static ByteBuffer buffer;
    private static final FtVarcharReader reader = new FtVarcharReader();

    @Before
    public void setUp() {
        buffer = ByteBuffer.allocate(100);
    }

    @Test
    public void readVarcharTest() {
        String[] expected = new String[AMOUNT];
        String value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_STRING + i;
            buffer.put(value.getBytes());
            expected[i] = value;
        }

        String result;
        buffer.flip();
        for (int i = 0; i < expected.length; i++) {
            result = reader.readVarchar(buffer, expected[i].length(), StandardCharsets.UTF_8.toString());
            assertEquals(expected[i], result);
        }
    }

    @Test
    public void readVarcharWithSpacePaddingTest() {
        String padding = "     ";
        testVarcharWithPadding(padding);
    }

    @Test
    public void readVarcharWithNullPaddingTest() {
        String padding = "\0\0\0\0\0";
        testVarcharWithPadding(padding);
    }

    private void testVarcharWithPadding(String padding) {
        String[] expected = new String[AMOUNT];
        String value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_STRING + i + padding;
            buffer.put(value.getBytes());
            expected[i] = value.trim();
        }

        String result;
        buffer.flip();
        for (int i = 0; i < expected.length; i++) {
            result = reader.readVarchar(buffer, expected[i].length() + padding.length(),
                    StandardCharsets.UTF_8.toString());

            assertEquals(expected[i], result);
        }
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