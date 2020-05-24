package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.sqream.jdbc.utils.Utils.calculateAllocation;

public class ByteBufferPool {
    private static final Logger LOGGER = Logger.getLogger(ByteBufferPool.class.getName());

    private BlockingQueue<BlockDto> queue;

    public ByteBufferPool(int queueSize, int blockSize, TableMetadata metadata) {
        this.queue = new ArrayBlockingQueue<>(queueSize);
        initByteBuffers(metadata, blockSize, queueSize);
    }

    public BlockDto getBlock() {
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Getting block from ByteBuffer pool. Blocks in the pool [{0}] (before taking)", queue.size()));
        try {
            return queue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void releaseBlock(BlockDto block) throws InterruptedException {
        queue.put(block);
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Released block in the pool. Blocks in the pool [{0}] (after releasing)", queue.size()));
    }

    private void initByteBuffers(TableMetadata metadata, int blockSize, int queueSize) {
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Start to init byteBuffer pool. Params: blockSize=[{0}], queueSize=[{1}]", blockSize, queueSize));
        for (int i = 0; i < queueSize; i++) {
            try {
                queue.put(createBlock(metadata, blockSize));
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted when add new block in queue", e);
            }
        }
        LOGGER.log(Level.FINE, MessageFormat.format("Initialized pool size = [{0}], Blocks in pool = [{1}]", queueSize, queue.size()));
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
        if (LOGGER.getParent().getLevel() == Level.FINE) {
            LOGGER.log(Level.FINE, MessageFormat.format("Created block. Allocated [{0}] Mb",
                    calculateAllocation(block) / (1024 * 1024)));
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
        return new BlockDto(dataColumns, nullColumns, nvarcLenColumns, blockSize);
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
