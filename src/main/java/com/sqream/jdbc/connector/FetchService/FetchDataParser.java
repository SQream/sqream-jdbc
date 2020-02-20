package com.sqream.jdbc.connector.FetchService;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.TableMetadata;

import java.nio.ByteBuffer;

public class FetchDataParser {

    public static BlockDto parse(ByteBuffer[] fetchBuffers, TableMetadata metadata, int rowsFetched) {
        ByteBuffer[] dataColumns = new ByteBuffer[metadata.getRowLength()];
        ByteBuffer[] nullColumns = new ByteBuffer[metadata.getRowLength()];
        ByteBuffer[] nvarcLenColumns = new ByteBuffer[metadata.getRowLength()];
        for (int idx = 0, buf_idx = 0; idx < metadata.getRowLength(); idx++, buf_idx++) {
            if (metadata.isNullable(idx)) {
                nullColumns[idx] = fetchBuffers[buf_idx];
                buf_idx++;
            } else {
                nullColumns[idx] = null;
            }
            if (metadata.isTruVarchar(idx)) {
                nvarcLenColumns[idx] = fetchBuffers[buf_idx];
                buf_idx++;
            } else {
                nvarcLenColumns[idx] = null;
            }
            dataColumns[idx] = fetchBuffers[buf_idx];
        }
        BlockDto resultBlock = new BlockDto(dataColumns, nullColumns, nvarcLenColumns, rowsFetched);
        resultBlock.setFillSize(rowsFetched);
        return resultBlock;
    }
}
