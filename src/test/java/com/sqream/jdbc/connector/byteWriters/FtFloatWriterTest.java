package com.sqream.jdbc.connector.byteWriters;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FtFloatWriterTest {
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(1000);
    private static final ByteWriter writer = new FtFloatWriter();

    @Test
    public void writeFloatFourBytesTest() {
        int expected = Float.BYTES;

        int result = writer.writeFloat(buffer, 1f);

        Assert.assertEquals(expected, result, 0);
    }
}