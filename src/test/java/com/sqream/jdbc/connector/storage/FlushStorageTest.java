package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.*;
import com.sqream.jdbc.connector.enums.StatementType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class FlushStorageTest {
    private static final int ROW_LENGTH = 1;
    private static final int BLOCK_SIZE = 2;

    @Test
    public void nextTest() throws ConnException {
        Storage storage = prepareStorage();
        int testInt = 1;

        storage.setInt(0, testInt);
        assertTrue(storage.next());
        storage.setInt(0, testInt);
        assertFalse(storage.next());

        storage.clearBuffers(ROW_LENGTH);

        storage.setInt(0, testInt);
        assertTrue(storage.next());
        storage.setInt(0, testInt);
        assertFalse(storage.next());
    }

    @Test(expected = ConnException.class)
    public void whenCallNextWithoutSettingAllColumnsThenThrowExceptionTest() throws ConnException {
        Storage storage = prepareStorage();

        assertTrue(storage.next());
    }

    @Test
    public void getBlockTest() throws ConnException {
        Storage storage = prepareStorage();
        int testInt = 1;
        storage.setInt(0, testInt);
        assertTrue(storage.next());
        storage.setInt(0, testInt);
        assertFalse(storage.next());

        BlockDto resultBlock = storage.getBlock();

        assertNotNull(resultBlock);
        assertEquals(BLOCK_SIZE, resultBlock.getCapacity());
        assertEquals(BLOCK_SIZE, resultBlock.getFillSize());
        assertNotNull(resultBlock.getDataBuffers());
        assertNotNull(resultBlock.getDataBuffers()[0]);
        resultBlock.getDataBuffers()[0].position(0);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            assertEquals(testInt, resultBlock.getDataBuffers()[0].getInt());
        }
    }

    private TableMetadata createMetadata() {
        ColumnMetadataDto columnMetadata =
                new ColumnMetadataDto(false, "name", false, "ftInt", Integer.BYTES);
        return TableMetadata.builder()
                .rowLength(ROW_LENGTH)
                .fromColumnsMetadata(Collections.singletonList(columnMetadata))
                .statementType(StatementType.INSERT)
                .build();
    }

    private BlockDto createBlock() {
        return new MemoryAllocationService()
                .buildBlock(createMetadata(), BLOCK_SIZE);
    }

    private Storage prepareStorage() {
        TableMetadata metadata = createMetadata();
        BlockDto block = createBlock();
        return new FlushStorage(metadata, block);
    }
}