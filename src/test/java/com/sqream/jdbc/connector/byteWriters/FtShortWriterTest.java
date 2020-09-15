package com.sqream.jdbc.connector.byteWriters;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FtShortWriterTest {
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(1000);
    private static final ByteWriter writer = new FtShortWriter();
    public static final int expected = Short.BYTES;

    @Test
    public void writeIntReturnsEightBytes() {

        int result = writer.writeInt(buffer, 1);

        Assert.assertEquals(expected, result, 0);
    }

    @Test
    public void writeLongReturnsEightBytes() {

        int result = writer.writeLong(buffer, 1L);

        Assert.assertEquals(expected, result, 0);
    }

    @Test
    public void writeShortReturnsEightBytes() {

        int result = writer.writeShort(buffer, (short) 1);

        Assert.assertEquals(expected, result, 0);
    }

    @Test
    public void writeUbyteReturnsEightBytes() {

        int result = writer.writeUbyte(buffer, (byte) 1);

        Assert.assertEquals(expected, result, 0);
    }
}