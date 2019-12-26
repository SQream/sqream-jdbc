package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;

public class BlockDto {

    private ByteBuffer[] dataBuffers;
    private ByteBuffer[] nullBuffers;
    private ByteBuffer[] nvarcLenBuffers;

    public BlockDto(ByteBuffer[] dataBuffers, ByteBuffer[] nullBuffers, ByteBuffer[] nvarcLenBuffers) {
        this.dataBuffers = dataBuffers;
        this.nullBuffers = nullBuffers;
        this.nvarcLenBuffers = nvarcLenBuffers;
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
