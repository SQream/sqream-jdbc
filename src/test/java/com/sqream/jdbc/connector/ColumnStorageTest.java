package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.storage.ColumnStorage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ColumnStorageTest {


    @Test
    public void getTotalLengthForHeaderTest() {
        int rowsPerFlush = 10;

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
                .blockSize(rowsPerFlush)
                .build();

        storage.setInt(0, 42);
        storage.setBoolean(0, true);
        storage.setLong(0, 123L);

        int expectedTotalLength = 103;
        int actualTotalLength = storage.getTotalLengthForHeader(rowLength, rowsPerFlush);

        assertEquals(expectedTotalLength, actualTotalLength);
    }
}