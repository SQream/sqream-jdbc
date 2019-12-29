package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class ColumnStorage {

    private ByteBuffer[] dataColumns;
    private ByteBuffer[] null_columns;
    private ByteBuffer[] nvarc_len_columns;
    private ByteBuffer null_resetter;
    private TableMetadata metadata;
    private int blockSize;

    private void init(TableMetadata metadata, int blockSize) {
        this.metadata = metadata;
        this.blockSize = blockSize;
        dataColumns = new ByteBuffer[metadata.getRowLength()];
        null_columns = new ByteBuffer[metadata.getRowLength()];
        nvarc_len_columns = new ByteBuffer[metadata.getRowLength()];
        setNullResetter();
    }

    private void setNullResetter() {
        null_resetter = ByteBuffer.allocate(blockSize);
    }

    public void initColumns(TableMetadata metadata, int blockSize) {
        init(metadata, blockSize);
        // Initiate buffers for each column using the metadata
        for (int idx = 0; idx < metadata.getRowLength(); idx++) {
            initDataColumns(idx, metadata.getSize(idx) * blockSize);
            if (metadata.isNullable(idx)) {
                initNullColumns(idx, blockSize);
            } else {
                null_columns[idx] = null;
            }
            if (metadata.isTruVarchar(idx)) {
                initNvarcLenColumns(idx, blockSize);
            } else {
                nvarc_len_columns[idx] = null;
            }
        }
    }

    public void load(ByteBuffer[] fetchBuffers, TableMetadata metadata, int blockSize) {
        init(metadata, blockSize);
        // Sort buffers to appropriate arrays (row_length determied during _query_type())
        for (int idx=0, buf_idx = 0; idx < metadata.getRowLength(); idx++, buf_idx++) {
            if(metadata.isNullable(idx)) {
                null_columns[idx] = fetchBuffers[buf_idx];
                buf_idx++;
            } else {
                null_columns[idx] = null;
            }
            if(metadata.isTruVarchar(idx)) {
                nvarc_len_columns[idx] = fetchBuffers[buf_idx];
                buf_idx++;
            } else {
                nvarc_len_columns[idx] = null;
            }
            dataColumns[idx] = fetchBuffers[buf_idx];
        }
    }

    public void loadBlock(BlockDto block) {
        this.dataColumns = block.getDataBuffers();
        this.null_columns = block.getNullBuffers();
        this.nvarc_len_columns = block.getNvarcLenBuffers();
    }

    private void initDataColumns(int index, int size) {
        dataColumns[index] = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void initNullColumns(int index, int size) {
        null_columns[index] = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void initNvarcLenColumns(int index, int size) {
        nvarc_len_columns[index] = ByteBuffer.allocateDirect(4 * size).order(ByteOrder.LITTLE_ENDIAN);
    }

    public void clearBuffers(int row_length) {
        for(int idx=0; idx < row_length; idx++) {
            if (null_columns[idx] != null) {
                // Clear doesn't actually nullify/reset the data
                null_columns[idx].clear();
                null_columns[idx].put(null_resetter);
                null_columns[idx].clear();
            }
            if(nvarc_len_columns[idx] != null)
                nvarc_len_columns[idx].clear();
            dataColumns[idx].clear();
        }
    }

    public int getTotalLengthForHeader(int row_length, int row_counter) {
        int total_bytes = 0;
        for(int idx=0; idx < row_length; idx++) {
            total_bytes += (null_columns[idx] != null) ? row_counter : 0;
            total_bytes += (nvarc_len_columns[idx] != null) ? 4 * row_counter : 0;
            total_bytes += dataColumns[idx].position();
        }
        return total_bytes;
    }

    public void setDataColumns(int index, ByteBuffer value) {
        dataColumns[index] = value;
    }

    public BlockDto getBlock() {
        return new BlockDto(dataColumns, null_columns, nvarc_len_columns);
    }

    public ByteBuffer getDataColumns(int index) {
        return dataColumns[index];
    }

    public ByteBuffer getNullColumn(int index) {
        return null_columns[index];
    }

    public ByteBuffer getNvarcLenColumn(int index) {
        return nvarc_len_columns[index];
    }

    public boolean isValueNotNull(int colNum, int rowCounter) {
        return null_columns[colNum] == null || null_columns[colNum].get(rowCounter) == 0;
    }

    public void setBoolean(int index, Boolean value) {
        if (value != null) {
            dataColumns[index].put((byte) ((value) ? 1 : 0));
        } else {
            dataColumns[index].put((byte) 0);
            markAsNull(index);
        }
    }

    public void setUbyte(int index, Byte value) {
        if (value != null) {
            dataColumns[index].put(value);
        } else {
            dataColumns[index].put((byte) 0);
            markAsNull(index);
        }
    }

    public void setShort(int index, Short value) {
        if (value != null) {
            dataColumns[index].putShort(value);
        } else {
            dataColumns[index].putShort((short) 0);
            markAsNull(index);
        }
    }

    public void setInt(int index, Integer value) {
        if (value != null) {
            dataColumns[index].putInt(value);
        } else {
            dataColumns[index].putInt(0);
            markAsNull(index);
        }
    }

    public void setLong(int index, Long value) {
        if (value != null) {
            dataColumns[index].putLong(value);
        } else {
            dataColumns[index].putLong(0L);
            markAsNull(index);
        }
    }

    public void setFloat(int index, Float value) {
        if (value != null) {
            dataColumns[index].putFloat(value);
        } else {
            dataColumns[index].putFloat(0f);
            markAsNull(index);
        }
    }

    public void setDouble(int index, Double value) {
        if (value != null) {
            dataColumns[index].putDouble(value);
        } else {
            dataColumns[index].putDouble(0d);
            markAsNull(index);
        }
    }

    public void setVarchar(int index, byte[] stringBytes, String originalString) {
        // Generate missing spaces to fill up to size
        byte [] spaces = new byte[metadata.getSize(index) - stringBytes.length];
        Arrays.fill(spaces, (byte) 32);  // ascii value of space
        // Set value and added spaces if needed
        dataColumns[index].put(stringBytes);
        dataColumns[index].put(spaces);
        if (originalString == null) {
            markAsNull(index);
        }
    }

    private void markAsNull(int index) {
        null_columns[index].put((byte) 1);
    }
}
