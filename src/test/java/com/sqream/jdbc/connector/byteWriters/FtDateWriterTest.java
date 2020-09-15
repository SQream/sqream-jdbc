package com.sqream.jdbc.connector.byteWriters;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FtDateWriterTest {
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(10);
    private static final ByteWriter writer = new FtDateWriter();

    @Test
    public void writeDateReturnsFourBytesTest() {
        int expected = Integer.BYTES;

        int result = writer.writeDate(buffer, 1);

        Assert.assertEquals(expected, result);
    }
}