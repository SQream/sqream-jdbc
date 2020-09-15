package com.sqream.jdbc.connector.byteWriters;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class FtDateTimeWriterTest {
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(10);
    private static final ByteWriter writer = new FtDateTimeWriter();

    @Test
    public void writeDateTimeReturnsEightBytesTest() {
        int expected = Long.BYTES;

        int result = writer.writeDateTime(buffer, (byte) 1);

        Assert.assertEquals(expected, result);
    }
}