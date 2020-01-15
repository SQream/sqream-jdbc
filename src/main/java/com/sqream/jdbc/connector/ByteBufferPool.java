package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ByteBufferPool {
    private static final Logger LOGGER = Logger.getLogger(ByteBufferPool.class.getName());

    private BlockingQueue<BlockDto> queue;
    private int blockSize;

    public ByteBufferPool(int queueSize, int blockSize, TableMetadata metadata) {
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.blockSize = blockSize;
        initByteBuffers(metadata, blockSize, queueSize);
    }

    public BlockDto getBlock() throws InterruptedException {
        return queue.take();
    }

    public void releaseBlock(BlockDto block) throws InterruptedException {
        queue.put(block);
    }

    private void initByteBuffers(TableMetadata metadata, int blockSize, int queueSize) {
        LOGGER.log(Level.FINE, "Init byteBuffer pool.");
        for (int i = 0; i < queueSize; i++) {
            try {
                queue.put(createBlock(metadata, blockSize));
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted when add new block in queue", e);
            }
        }
        LOGGER.log(Level.FINE, MessageFormat.format("Pool size = [{0}], Blocks in pool = [{1}]", queueSize, queue.size()));
    }

    private BlockDto createBlock(TableMetadata metadata, int blockSize) {
        BlockDto block = initBlock(metadata, blockSize);
        // Initiate buffers for each column using the metadata
        for (int idx = 0; idx < metadata.getRowLength(); idx++) {
            block.getDataBuffers()[idx] = initDataColumns(metadata.getSize(idx) * blockSize);
            if (metadata.isNullable(idx)) {
                block.getNullBuffers()[idx] = initNullColumns(blockSize);
            } else {
                block.getNullBuffers()[idx] = null;
            }
            if (metadata.isTruVarchar(idx)) {
                block.getNvarcLenBuffers()[idx] = initNvarcLenColumns(blockSize);
            } else {
                block.getNvarcLenBuffers()[idx] = null;
            }
        }

        if (LOGGER.getParent().getLevel() == Level.FINEST) {
            LOGGER.log(Level.FINEST, "Created block");
            logBlockInfo(block);
        }
        return block;
    }

    private void logBlockInfo(BlockDto block) {
        for (ByteBuffer buffer : block.getDataBuffers()) {
            LOGGER.log(Level.FINEST, String.format("dataBuffers: %s", buffer));
        }
        for (ByteBuffer buffer : block.getNullBuffers()) {
            LOGGER.log(Level.FINEST, String.format("nullBuffers: %s", buffer));
        }
        for (ByteBuffer buffer : block.getNvarcLenBuffers()) {
            LOGGER.log(Level.FINEST, String.format("nvarcLenBuffers: %s", buffer));
        }
    }

    private BlockDto initBlock(TableMetadata metadata, int blockSize) {
        ByteBuffer[] dataColumns = new ByteBuffer[metadata.getRowLength()];
        ByteBuffer[] nullColumns = new ByteBuffer[metadata.getRowLength()];
        ByteBuffer[] nvarcLenColumns = new ByteBuffer[metadata.getRowLength()];
        return new BlockDto(dataColumns, nullColumns, nvarcLenColumns);
    }

    private ByteBuffer initDataColumns(int size) {
        return ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    private ByteBuffer initNullColumns(int size) {
        return ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    private ByteBuffer initNvarcLenColumns(int size) {
        return ByteBuffer.allocateDirect(4 * size).order(ByteOrder.LITTLE_ENDIAN);
    }
}
