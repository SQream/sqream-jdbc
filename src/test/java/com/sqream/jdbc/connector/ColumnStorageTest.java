package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.storage.FlushStorage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.sqream.jdbc.connector.JsonParser.TEXT_ITEM_SIZE;
import static org.junit.Assert.*;

public class ColumnStorageTest {

    @Test
    public void increaseBufferTextBufferTest() throws ConnException {
        int rowLength = 1;
        int blockSize = 1;
        List<ColumnMetadataDto> columnMetadataDtos =  Collections.singletonList(
                new ColumnMetadataDto(true, "testName", false, "ftBlob", 0));
        TableMetadata metadata = TableMetadata.builder()
                .rowLength(rowLength)
                .fromColumnsMetadata(columnMetadataDtos)
                .statementType(StatementType.INSERT)
                .build();

        FlushStorage storage =
                new FlushStorage(metadata, new MemoryAllocationService().buildBlock(metadata, blockSize));

        String sampleText = "1";
        String testString = String.join("", Collections.nCopies(TEXT_ITEM_SIZE * 3, sampleText));

        storage.setNvarchar(0, testString.getBytes(), testString);
    }
}