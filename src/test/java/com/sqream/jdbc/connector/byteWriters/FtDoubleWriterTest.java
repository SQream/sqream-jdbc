package com.sqream.jdbc.connector.byteWriters;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FtDoubleWriterTest {
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(1000);
    private static final ByteWriter writer = new FtDoubleWriter();

    @Test
    public void writeDoubleReturnsEightBytes() {
        int expected = Double.BYTES;

        int result = writer.writeDouble(buffer, 1d);

        Assert.assertEquals(expected, result, 0);
    }

}