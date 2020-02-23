package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.*;
import com.sqream.jdbc.connector.enums.StatementType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class FetchStorageTest {
    private static final int ROW_LENGTH = 1;
    private static final int BLOCK_SIZE = 2;

    @Test(expected = UnsupportedOperationException.class)
    public void getBlockTest() {
        Storage storage = prepareStorage();

        storage.getBlock();
    }

    @Test
    public void nextTest() throws ConnException {
        Storage storage = prepareStorage();
        BlockDto secondBlock = createBlock();

        assertTrue(storage.next());
        assertEquals(1, storage.getInt(0), 0);
        assertTrue(storage.next());
        assertEquals(1, storage.getInt(0), 0);
        assertFalse(storage.next());

        storage.setBlock(secondBlock);

        assertTrue(storage.next());
        assertEquals(1, storage.getInt(0), 0);
        assertTrue(storage.next());
        assertEquals(1, storage.getInt(0), 0);
        assertFalse(storage.next());
    }

    private TableMetadata createMetadata() {
        ColumnMetadataDto columnMetadata =
                new ColumnMetadataDto(false, "name", false, "ftInt", Integer.BYTES);
        return TableMetadata.builder()
                .rowLength(ROW_LENGTH)
                .fromColumnsMetadata(Collections.singletonList(columnMetadata))
                .statementType(StatementType.SELECT)
                .build();
    }

    private BlockDto createBlock() {
        BlockDto block = new MemoryAllocationService()
                .buildBlock(createMetadata(), BLOCK_SIZE * Integer.BYTES);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block.getDataBuffers()[0].putInt(1);
        }
        block.setFillSize(BLOCK_SIZE);
        return block;
    }

    private Storage prepareStorage() {
        TableMetadata metadata = createMetadata();
        BlockDto block = createBlock();
        return new FetchStorage(metadata, block);
    }
}