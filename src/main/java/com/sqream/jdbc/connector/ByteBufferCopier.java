package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;

public class ByteBufferCopier {

    public BlockDto copyBlock(BlockDto block) {
        if (block == null) {
            return null;
        }
        ByteBuffer[] dataBuffers = block.getDataBuffers();
        ByteBuffer[] nullBuffers = block.getNullBuffers();
        ByteBuffer[] nvarcLenBuffers = block.getNvarcLenBuffers();

        ByteBuffer[] targetDataBuffers = new ByteBuffer[dataBuffers.length];
        ByteBuffer[] targetNullBuffers = new ByteBuffer[nullBuffers.length];
        ByteBuffer[] targetNvarcLenBuffers = new ByteBuffer[nvarcLenBuffers.length];

        for (int i = 0; i < block.getDataBuffers().length; i++) {
            targetDataBuffers[i] = copyByteBuffer(dataBuffers[i]);
        }
        for (int i = 0; i < block.getNullBuffers().length; i++) {
            targetNullBuffers[i] = copyByteBuffer(nullBuffers[i]);
        }
        for (int i = 0; i < block.getNvarcLenBuffers().length; i++) {
            targetNvarcLenBuffers[i] = copyByteBuffer(nvarcLenBuffers[i]);
        }
        return new BlockDto(targetDataBuffers, targetNullBuffers, targetNvarcLenBuffers);
    }

    public ByteBuffer copyByteBuffer(ByteBuffer original) {
        if (original == null) {
            return null;
        }
        final ByteBuffer clone = (original.isDirect()) ?
                ByteBuffer.allocateDirect(original.capacity()) :
                ByteBuffer.allocate(original.capacity());

        final ByteBuffer readOnlyCopy = original.asReadOnlyBuffer();

        readOnlyCopy.flip();
        clone.put(readOnlyCopy);

        clone.position(original.position());
        clone.limit(original.limit());
        clone.order(original.order());

        return clone;
    }
}
