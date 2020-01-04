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
        int[] expected = new int[AMOUNT];
        int value;
        for (int i = 0; i < AMOUNT; i++) {
            value = TEST_INT + i;
            buffer.putInt(value);
            expected[i] = value;
        }

        readAndCheckInt(expected, "ftInt");
    }

    @Test
    public void readIntFromFtShortTest() {
        int[] expected = new int[AMOUNT];
        short value;
        for (int i = 0; i < AMOUNT; i++) {
            value = (short) (TEST_INT + i);
            buffer.putShort(value);
            expected[i] = value;
        }

        readAndCheckInt(expected, "ftShort");
    }

    @Test
    public void readIntFromFtUByteTest() {
        int[] expected = new int[AMOUNT];
        byte value;
        for (int i = 0; i < AMOUNT; i++) {
            value = (byte) i;
            buffer.put(value);
            expected[i] = value;
        }

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

    @Test(expected = UnsupportedOperationException.class)
    public void readShortFromFtIntTest() {
        String expectedMessage = "Trying to get a value of type [Short] from column type [ftInt]";
        try {
            ByteReaderFactory
                    .getReader("ftInt")
                    .readShort(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test
    public void readShortFromFtShortTest() {
        short[] expected = new short[AMOUNT];
        short value;
        for (int i = 0; i < AMOUNT; i++) {
            value = (short) (TEST_INT + i);
            buffer.putShort(value);
            expected[i] = value;
        }

        readAndCheckShort(expected, "ftShort");
    }

    @Test
    public void readShortFromFtUByteTest() {
        short[] expected = new short[AMOUNT];
        for (int i = 0; i < AMOUNT; i++) {
            buffer.put((byte) i);
            expected[i] = (short) i;
        }

        readAndCheckShort(expected, "ftUByte");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readShortFromFtLongTest() {
        String expectedMessage = "Trying to get a value of type [Short] from column type [ftLong]";
        try {
            ByteReaderFactory
                    .getReader("ftLong")
                    .readShort(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readShortFromFtFloatTest() {
        String expectedMessage = "Trying to get a value of type [Short] from column type [ftFloat]";
        try {
            ByteReaderFactory
                    .getReader("ftFloat")
                    .readShort(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readShortFromFtDoubleTest() {
        String expectedMessage = "Trying to get a value of type [Short] from column type [ftDouble]";
        try {
            ByteReaderFactory
                    .getReader("ftDouble")
                    .readShort(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readShortFromFtDateTest() {
        String expectedMessage = "Trying to get a value of type [Short] from column type [ftDate]";
        try {
            ByteReaderFactory
                    .getReader("ftDate")
                    .readShort(buffer, 0);
        } catch (UnsupportedOperationException e) {
            if (expectedMessage.equals(e.getMessage())) {
                throw e;
            }
            fail("Incorrect exception message");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void readShortFromFtDateTimeTest() {
        String expectedMessage = "Trying to get a value of type [Short] from column type [ftDateTime]";
        try {
            ByteReaderFactory
                    .getReader("ftDateTime")
                    .readShort(buffer, 0);
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

    private void readAndCheckShort(short[] expected, String columnType) {
        for (int i = 0; i < expected.length; i++) {
            short result = ByteReaderFactory
                    .getReader(columnType)
                    .readShort(buffer, i);

            Assert.assertEquals(expected[i], result);
        }
    }
}