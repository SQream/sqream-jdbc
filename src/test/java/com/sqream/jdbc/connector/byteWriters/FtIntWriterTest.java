package com.sqream.jdbc.connector.byteWriters;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class FtIntWriterTest {
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(1000);
    private static final ByteWriter writer = new FtIntWriter();
    private static final int expected = Integer.BYTES;

    @Test
    public void writeIntReturnsFourBytes() {

        int result = writer.writeInt(buffer, 1);

        Assert.assertEquals(expected, result, 0);
    }

    @Test
    public void writeLongReturnsFourBytes() {

        int result = writer.writeLong(buffer, 1L);

        Assert.assertEquals(expected, result, 0);
    }

    @Test
    public void writeShortReturnsFourBytes() {

        int result = writer.writeShort(buffer, (short) 1);

        Assert.assertEquals(expected, result, 0);
    }

    @Test
    public void writeUbyteReturnsFourBytes() {

        int result = writer.writeUbyte(buffer, (byte) 1);

        Assert.assertEquals(expected, result, 0);
    }
}