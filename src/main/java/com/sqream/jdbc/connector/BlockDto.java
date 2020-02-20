package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;

public class BlockDto {

    private ByteBuffer[] dataBuffers;
    private ByteBuffer[] nullBuffers;
    private ByteBuffer[] nvarcLenBuffers;
    private int capacity;
    private int fillSize = 0;

    public BlockDto(ByteBuffer[] dataBuffers, ByteBuffer[] nullBuffers, ByteBuffer[] nvarcLenBuffers, int capacity) {
        this.dataBuffers = dataBuffers;
        this.nullBuffers = nullBuffers;
        this.nvarcLenBuffers = nvarcLenBuffers;
        this.capacity = capacity;
    }

    public ByteBuffer[] getDataBuffers() {
        return dataBuffers;
    }

    public void setDataBuffers(ByteBuffer[] dataBuffers) {
        this.dataBuffers = dataBuffers;
    }

    public ByteBuffer[] getNullBuffers() {
        return nullBuffers;
    }

    public void setNullBuffers(ByteBuffer[] nullBuffers) {
        this.nullBuffers = nullBuffers;
    }

    public ByteBuffer[] getNvarcLenBuffers() {
        return nvarcLenBuffers;
    }

    public void setNvarcLenBuffers(ByteBuffer[] nvarcLenBuffers) {
        this.nvarcLenBuffers = nvarcLenBuffers;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getFillSize() {
        return fillSize;
    }

    public void setFillSize(int fillSize) {
        this.fillSize = fillSize;
    }
}
