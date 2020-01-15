package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.storage.ColumnStorage;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ByteBufferPoolTest {

    @Test
    public void getBlockReturnBuffersWithTheSameParamsTest() throws InterruptedException {
        int blockSize = 3;
        List<ColumnMetadataDto> columnMetadataList = new ArrayList<>();
        columnMetadataList.add(new ColumnMetadataDto(true, "testColName0", true, "ftInt", 4));
        columnMetadataList.add(new ColumnMetadataDto(false, "testColName1", false, "ftBool", 1));
        columnMetadataList.add(new ColumnMetadataDto(true, "testColName1", false, "ftLong", 8));
        int rowLength = columnMetadataList.size();

        TableMetadata metadata = TableMetadata.builder()
                .rowLength(rowLength)
                .fromColumnsMetadata(columnMetadataList)
                .statementType(StatementType.INSERT)
                .build();

        ColumnStorage storage = ColumnStorage.builder()
                .metadata(metadata)
                .blockSize(blockSize)
                .build();

        BlockDto blockFromStorage = storage.getBlock();

        ByteBufferPool pool = new ByteBufferPool(1, 3, metadata);
        BlockDto blockFromPool = pool.getBlock();

        assertNotNull(blockFromPool);
        assertNotNull(blockFromStorage);
        assertNotEquals(blockFromPool, blockFromStorage);
        checkBlocksHaveTheSameParams(blockFromPool, blockFromStorage);
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