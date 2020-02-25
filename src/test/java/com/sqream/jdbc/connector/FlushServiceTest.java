package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.messenger.MessengerImpl;
import com.sqream.jdbc.connector.storage.FlushStorage;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class FlushServiceTest {

    @Test
    public void asyncProcessDoesNotAffectReturnedBlockTest() throws InterruptedException {
        int rowLength = 5;
        int rowCounter = 10;

        List<ColumnMetadataDto> columnMetadataList = createColumnMetadataList(rowLength);
        TableMetadata metadata = TableMetadata.builder()
                .rowLength(rowLength)
                .fromColumnsMetadata(columnMetadataList)
                .statementType(StatementType.INSERT)
                .build();
        FlushStorage storage = new FlushStorage(metadata, new MemoryAllocationService().buildBlock(metadata, rowCounter));
        BlockDto block = storage.getBlock();
        SQSocketConnector socketConnector = Mockito.mock(SQSocketConnector.class);
        ByteBufferPool bufferPool = new ByteBufferPool(1, rowCounter, metadata);
        FlushService flushService = FlushService.getInstance(socketConnector, MessengerImpl.getInstance(socketConnector));
        flushService.process(metadata, block, bufferPool);

        ByteBufferPool pool = new ByteBufferPool(2, rowCounter, metadata);
        BlockDto blockFromPool = pool.getBlock();

        changeBuffer(block.getDataBuffers());
        changeBuffer(block.getNullBuffers());
        changeBuffer(block.getNvarcLenBuffers());


        compareBlocks(block, blockFromPool);
    }

    private List<ColumnMetadataDto> createColumnMetadataList(int rowLength) {
        List<ColumnMetadataDto> columnMetadataList = new ArrayList<>(rowLength);
        for (int i = 0; i < rowLength; i++) {
            String testName = String.format("COL_%s_TEST_NAME", i + 1);
            String testType = String.format("COL_%s_TEST_TYPE", i + 1);
            int curSize = 10 + i;
            boolean isNullable = i % 2 == 0;
            boolean isTruVarchar = i % 2 == 0;
            columnMetadataList.add(new ColumnMetadataDto(isTruVarchar, testName, isNullable, testType, curSize));
        }
        return columnMetadataList;
    }

    private void changeBuffer(ByteBuffer[] buffers) {
        for (int i = 0; i < buffers.length; i++) {
            if (buffers[i] != null) {
                buffers[i].put((byte) 1);
            }
        }
    }

    private void compareBlocks(BlockDto originalBlock, BlockDto returnedBlock) {
        assertNotNull(originalBlock);
        assertNotNull(returnedBlock);

        compareBuffersPositions(originalBlock.getDataBuffers(), returnedBlock.getDataBuffers());
        compareBuffersPositions(originalBlock.getNullBuffers(), returnedBlock.getNullBuffers());
        compareBuffersPositions(originalBlock.getNullBuffers(), returnedBlock.getNullBuffers());
    }

    private void compareBuffersPositions(ByteBuffer[] fromOriginalBlock, ByteBuffer[] fromReturnedBlock) {
        for (int i = 0; i < fromOriginalBlock.length; i++) {
            assertNotNull(fromOriginalBlock);
            assertNotNull(fromReturnedBlock);
            if (fromOriginalBlock[i] != null && fromReturnedBlock[i] != null) {
                assertNotEquals(fromOriginalBlock[i].position(), fromReturnedBlock[i].position());
                assertNotEquals(fromOriginalBlock[i].get(0), fromReturnedBlock[i].get(0));
            }
        }
    }
}