package com.sqream.jdbc.connector.byteWriters;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FtBooleanWriterTest {
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(10);
    private static final ByteWriter writer = new FtBooleanWriter();

    @Test
    public void writeBooleanReturnsOneByteTest() {
        int expected = Byte.BYTES;

        int result = writer.writeBoolean(buffer, (byte) 1);

        Assert.assertEquals(expected, result);
    }
}