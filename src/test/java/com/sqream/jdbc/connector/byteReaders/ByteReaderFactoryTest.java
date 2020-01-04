package com.sqream.jdbc.connector.byteReaders;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.fail;

public class ByteReaderFactoryTest {

    private static final int TEST_INT = 123;
    private static final int AMOUNT = 3;
    private static ByteBuffer buffer;



    @Before
    public void setUp() {
        buffer = ByteBuffer.allocate(30);
    }

    @Test
    public void readIntFromFtIntTest() {
        int[] expected = putInts(AMOUNT);
        readAndCheckInt(expected, "ftInt");
    }

    @Test
    public void readIntFromFtShortTest() {
        int[] expected = putShorts(AMOUNT);
        readAndCheckInt(expected, "ftShort");
    }

    @Test
    public void readIntFromFtUByteTest() {
        int[] expected = putUBytes(AMOUNT);
        readAndCheckInt(expected, "ftUByte");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readIntFromFtLongTest() {
        String expectedMessage = "Trying to get a value of type [Integer] from column type [ftLong]";
        try {
            ByteReaderFactory
                    .getReader("ftLong")
                    .readInt(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readIntFromFtFloatTest() {
        String expectedMessage = "Trying to get a value of type [Integer] from column type [ftFloat]";
        try {
            ByteReaderFactory
                    .getReader("ftFloat")
                    .readInt(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readIntFromFtDoubleTest() {
        String expectedMessage = "Trying to get a value of type [Integer] from column type [ftDouble]";
        try {
            ByteReaderFactory
                    .getReader("ftDouble")
                    .readInt(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readIntFromFtDateTest() {
        String expectedMessage = "Trying to get a value of type [Integer] from column type [ftDate]";
        try {
            ByteReaderFactory
                    .getReader("ftDate")
                    .readInt(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readIntFromFtDateTimeTest() {
        String expectedMessage = "Trying to get a value of type [Integer] from column type [ftDateTime]";
        try {
            ByteReaderFactory
                    .getReader("ftDateTime")
                    .readInt(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void readFromUnsupportedColumnType() {
        ByteReaderFactory.getReader("someUnsupportedColumnType");
    }

    private void readAndCheckInt(int[] expected, String columnType) {
        for (int i = 0; i < expected.length; i++) {
            int result = ByteReaderFactory
                    .getReader(columnType)
                    .readInt(buffer, i);

            Assert.assertEquals(expected[i], result);
        }
    }

    private int[] putInts(int amount) {
        int[] result = new int[amount];
        int testValue;
        for (int i = 0; i < amount; i++) {
            testValue = TEST_INT + i;
            buffer.putInt(testValue);
            result[i] = testValue;
        }
        return result;
    }

    private int[] putUBytes(int amount) {
        int[] result = new int[amount];
        byte testValue;
        for (int i = 0; i < amount; i++) {
            testValue = (byte) (i);
            buffer.put(testValue);
            result[i] = testValue;
        }
        return result;
    }

    private int[] putLongs(int amount) {
        int[] result = new int[amount];
        long testValue;
        for (int i = 0; i < amount; i++) {
            testValue = (byte) (i);
            buffer.putLong(testValue);
            result[i] = (int) testValue;
        }
        return result;
    }

    private int[] putShorts(int amount) {
        int[] result = new int[amount];
        short testValue;
        for (int i = 0; i < amount; i++) {
            testValue = (short) (TEST_INT + i);
            buffer.putShort(testValue);
            result[i] = testValue;
        }
        return result;
    }
}