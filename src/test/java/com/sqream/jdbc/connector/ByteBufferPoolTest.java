package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.storage.FlushStorage;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static com.sqream.jdbc.connector.ConnectorImpl.BYTES_PER_FLUSH_LIMIT;
import static org.junit.Assert.*;

public class ByteBufferPoolTest {

    @Test
    public void getBlockReturnBuffersWithTheSameParamsTest() throws InterruptedException {
        int blockSize = 3;
        int rowLength = 3;

        TableMetadata metadata = createTableMetadata(rowLength);

        FlushStorage storage = new FlushStorage(metadata, new MemoryAllocationService().buildBlock(metadata, blockSize), BYTES_PER_FLUSH_LIMIT);

        BlockDto blockFromStorage = storage.getBlock();

        ByteBufferPool pool = new ByteBufferPool(1, blockSize, metadata);
        BlockDto blockFromPool = pool.getBlock();

        assertNotNull(blockFromPool);
        assertNotNull(blockFromStorage);
        assertNotEquals(blockFromPool, blockFromStorage);
        checkBlocksHaveTheSameParams(blockFromPool, blockFromStorage);
    }

    @Test
    public void whenReleaseAndGetBlockThenBlocksAndArraysHaveDifferentHashCodesTest() throws InterruptedException {
        int blockSize = 3;
        int rowLength = 3;

        TableMetadata metadata = createTableMetadata(rowLength);

        FlushStorage storage = new FlushStorage(metadata, new MemoryAllocationService().buildBlock(metadata, blockSize), BYTES_PER_FLUSH_LIMIT);

        BlockDto blockFromStorage = storage.getBlock();
        int blockFromStorageHashCode = blockFromStorage.hashCode();

        ByteBufferPool pool = new ByteBufferPool(1, blockSize, metadata);

        BlockDto blockFromPool = pool.getBlock();
        int blockFromPoolHashCode = blockFromPool.hashCode();

        assertNotEquals(blockFromPoolHashCode, blockFromStorageHashCode);
        assertNotEquals(blockFromPool.getDataBuffers(), blockFromStorage.getDataBuffers());
        assertNotEquals(blockFromPool.getNullBuffers(), blockFromStorage.getNullBuffers());
        assertNotEquals(blockFromPool.getNvarcLenBuffers(), blockFromStorage.getNvarcLenBuffers());
    }

    private TableMetadata createTableMetadata(int excpectedRowLength) {
        List<ColumnMetadataDto> columnMetadataList = new ArrayList<>();
        columnMetadataList.add(new ColumnMetadataDto(true, "testColName0", true, "ftInt", 4, 0, 0));
        columnMetadataList.add(new ColumnMetadataDto(false, "testColName1", false, "ftBool", 1, 0, 0));
        columnMetadataList.add(new ColumnMetadataDto(true, "testColName1", false, "ftLong", 8, 0, 0));
        int rowLength = columnMetadataList.size();

        if (excpectedRowLength != rowLength) {
            throw new IllegalArgumentException(MessageFormat.format("Method generate metadata only for rowLength == [{0}]", rowLength));
        }

        return TableMetadata.builder()
                .rowLength(rowLength)
                .fromColumnsMetadata(columnMetadataList)
                .statementType(StatementType.NETWORK_INSERT)
                .build();
    }

    private void checkBlocksHaveTheSameParams(BlockDto expectedBlock, BlockDto originalBlock) {
        assertEquals(expectedBlock.getDataBuffers().length, originalBlock.getDataBuffers().length);
        assertEquals(expectedBlock.getNullBuffers().length, originalBlock.getNullBuffers().length);
        assertEquals(expectedBlock.getNvarcLenBuffers().length, originalBlock.getNvarcLenBuffers().length);

        checkByteBufferArrayHaveTheSameParams(expectedBlock.getDataBuffers(), originalBlock.getDataBuffers());
        checkByteBufferArrayHaveTheSameParams(expectedBlock.getNullBuffers(), originalBlock.getNullBuffers());
        checkByteBufferArrayHaveTheSameParams(expectedBlock.getNvarcLenBuffers(), originalBlock.getNvarcLenBuffers());
    }

    private void checkByteBufferArrayHaveTheSameParams(ByteBuffer[] expectedBuffer, ByteBuffer[] originalBuffer) {
        for (int i = 0; i < expectedBuffer.length; i++) {
            if (expectedBuffer[i] == null || originalBuffer[i] == null) {
                assertTrue(expectedBuffer[i] == null && originalBuffer[i] == null);
            } else {
                assertEquals(expectedBuffer[i].capacity(), originalBuffer[i].capacity());
                assertEquals(expectedBuffer[i].limit(), originalBuffer[i].limit());
            }
        }
    }
}
