package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ByteBufferPool {

    private BlockingQueue<BlockDto> queue;
    private ByteBufferCopier copier;
    private int blockSize;

    public ByteBufferPool(int queueSize, ByteBufferCopier copier, int blockSize) {
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.blockSize = blockSize;
    }

    public BlockDto getBlock() throws InterruptedException {
            return queue.take();
    }

    public void releaseBlock(BlockDto block) throws InterruptedException {
        clearBuffers(block);
        queue.put(block);
    }

    private void clearBuffers(BlockDto block) {
        if (block == null || block.getDataBuffers() == null) {
            return;
        }

        ByteBuffer[] dataBuffers = block.getDataBuffers();
        ByteBuffer[] nullBuffers = block.getNullBuffers();
        ByteBuffer[] nvarcLenBuffers = block.getNvarcLenBuffers();

        int rowLength = dataBuffers.length;
        for(int idx=0; idx < rowLength; idx++) {
            if (block.getNullBuffers() != null &&  block.getNullBuffers()[idx] != null) {
                // Clear doesn't actually nullify/reset the data
                nullBuffers[idx].clear();
                nullBuffers[idx].put(ByteBuffer.allocate(blockSize));
                nullBuffers[idx].clear();
            }
            if(nvarcLenBuffers[idx] != null)
                nvarcLenBuffers[idx].clear();
            dataBuffers[idx].clear();
        }
    }
}
