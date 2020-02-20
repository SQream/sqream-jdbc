package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Logger;

public class MemoryAllocationService {
    private static final Logger LOGGER = Logger.getLogger(MemoryAllocationService.class.getName());

    private ByteBuffer[] dataColumns;
    private ByteBuffer[] nullColumns;
    private ByteBuffer[] nvarcLenColumns;
    private TableMetadata metadata;
    private int blockSize;

    public BlockDto buildBlock(TableMetadata metadata, int blockSize) {
        initArrays(metadata, blockSize);
        // Initiate buffers for each column using the metadata
        for (int idx = 0; idx < metadata.getRowLength(); idx++) {
            initDataColumns(idx, metadata.getSize(idx) * blockSize);
            if (metadata.isNullable(idx)) {
                initNullColumns(idx, blockSize);
            } else {
                nullColumns[idx] = null;
            }
            if (metadata.isTruVarchar(idx)) {
                initNvarcLenColumns(idx, blockSize);
            } else {
                nvarcLenColumns[idx] = null;
            }
        }
        return new BlockDto(dataColumns, nullColumns, nvarcLenColumns, blockSize);
    }

    private void initArrays(TableMetadata metadata, int blockSize) {
        this.metadata = metadata;
        this.blockSize = blockSize;
        dataColumns = new ByteBuffer[metadata.getRowLength()];
        nullColumns = new ByteBuffer[metadata.getRowLength()];
        nvarcLenColumns = new ByteBuffer[metadata.getRowLength()];
    }

    private void initDataColumns(int index, int size) {
        dataColumns[index] = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void initNullColumns(int index, int size) {
        nullColumns[index] = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void initNvarcLenColumns(int index, int size) {
        nvarcLenColumns[index] = ByteBuffer.allocateDirect(4 * size).order(ByteOrder.LITTLE_ENDIAN);
    }
}
