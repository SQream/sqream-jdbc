package com.sqream.jdbc.connector.byteWriters;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FtNvarcharWriterTest {
    private static final ByteBuffer buffer = ByteBuffer.allocateDirect(1000);
    private static final ByteWriter writer = new FtNvarcharWriter();

    @Test
    public void writeNvarcharReturnsEightBytes() {
        String testText = "abcdefghigklmnopqrstuvwxyz";
        int expected = testText.length();

        int result = writer.writeNvarchar(buffer, testText.getBytes());

        Assert.assertEquals(expected, result, 0);
    }

}