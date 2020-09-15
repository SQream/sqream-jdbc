package com.sqream.jdbc.connector.byteWriters;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FtVarcharWriterTest {
    private static ByteBuffer buffer = ByteBuffer.allocateDirect(1000);

    @Test
    public void writeNvarcharReturnsEightBytes() {
        int colLength = 10;
        String testText = "abc";

        ByteWriter writer = new FtVarcharWriter();
        int result = writer.writeVarchar(buffer, testText.getBytes(), colLength);

        Assert.assertEquals(colLength, result, 0);
    }

}