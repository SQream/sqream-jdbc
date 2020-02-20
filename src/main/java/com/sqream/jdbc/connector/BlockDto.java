package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;

public class BlockDto {

    private ByteBuffer[] dataBuffers;
    private ByteBuffer[] nullBuffers;
    private ByteBuffer[] nvarcLenBuffers;
    private int fillSize = 0;

    public BlockDto(ByteBuffer[] dataBuffers, ByteBuffer[] nullBuffers, ByteBuffer[] nvarcLenBuffers) {
        this.dataBuffers = dataBuffers;
        this.nullBuffers = nullBuffers;
        this.nvarcLenBuffers = nvarcLenBuffers;
    }

    public void setFillSize(int fillSize) {
        this.fillSize = fillSize;
    }

    public int getFillSize() {
        return fillSize;
    }

    public ByteBuffer[] getDataBuffers() {
        return dataBuffers;
    }

    public ByteBuffer[] getNullBuffers() {
        return nullBuffers;
    }

    public ByteBuffer[] getNvarcLenBuffers() {
        return nvarcLenBuffers;
    }
}
