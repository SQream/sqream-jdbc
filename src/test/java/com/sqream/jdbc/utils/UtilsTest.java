package com.sqream.jdbc.utils;

import com.sqream.jdbc.connector.*;
import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.storage.FlushStorage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class UtilsTest {

    @Test
    public void getTotalLengthForHeaderTest() throws ConnException {
        int rowsPerFlush = 10;
        long expectedTotalLength = 0;

        List<ColumnMetadataDto> columnMetadataList = new ArrayList<>();

        columnMetadataList.add(new ColumnMetadataDto(true, "testColName0", true, "ftInt", 4));
        expectedTotalLength += (Integer.BYTES + Byte.BYTES + Integer.BYTES);

        columnMetadataList.add(new ColumnMetadataDto(false, "testColName1", false, "ftBool", 1));
        expectedTotalLength += (0 + 0 + Byte.BYTES);

        columnMetadataList.add(new ColumnMetadataDto(true, "testColName1", false, "ftLong", 8));
        expectedTotalLength += (Integer.BYTES + 0 + Long.BYTES);

        int rowLength = columnMetadataList.size();

        TableMetadata metadata = TableMetadata.builder()
                .rowLength(rowLength)
                .fromColumnsMetadata(columnMetadataList)
                .statementType(StatementType.INSERT)
                .build();

        FlushStorage storage =
                new FlushStorage(metadata, new MemoryAllocationService().buildBlock(metadata, rowsPerFlush));

        storage.setInt(0, 42);
        storage.setBoolean(1, true);
        storage.setLong(2, 123L);
        storage.next();

        long actualTotalLength = Utils.totalLengthForHeader(metadata, storage.getBlock());

        assertEquals(expectedTotalLength, actualTotalLength);
    }

}