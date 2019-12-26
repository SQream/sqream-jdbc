package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ColumnStorage {

    private ByteBuffer[] data_columns;
    private ByteBuffer[] null_columns;
    private ByteBuffer[] nvarc_len_columns;
    private ByteBuffer null_resetter;
    private ColumnsMetadata metadata;
    private int blockSize;



    public void init(ColumnsMetadata metadata, int blockSize) {
        this.metadata = metadata;
        this.blockSize = blockSize;
        data_columns = new ByteBuffer[metadata.getRowLength()];
        null_columns = new ByteBuffer[metadata.getRowLength()];
        nvarc_len_columns = new ByteBuffer[metadata.getRowLength()];
        setNullResetter();
    }

    private void setNullResetter() {
        null_resetter = ByteBuffer.allocate(blockSize);
    }

    public void initColumns() {
        // Initiate buffers for each column using the metadata
        for (int idx = 0; idx < metadata.getRowLength(); idx++) {
            initDataColumns(idx, metadata.getSize(idx) * blockSize);
            if (metadata.isNullable(idx)) {
                initNullColumns(idx, blockSize);
            } else {
                resetNullColumns(idx);
            }
            if (metadata.isTruVarchar(idx)) {
                initNvarcLenColumns(idx, blockSize);
            } else {
                resetNvarcLenColumns(idx);
            }
        }
    }

    public void reload(ByteBuffer[] dataBuffer, ByteBuffer[] nullBuffer, ByteBuffer[] nvarcLenBuffer) {
        this.data_columns = dataBuffer;
        this.null_columns = nullBuffer;
        this.nvarc_len_columns = nvarcLenBuffer;
    }

    private void initDataColumns(int index, int size) {
        data_columns[index] = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void initNullColumns(int index, int size) {
        null_columns[index] = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    public void resetNullColumns(int index) {
        null_columns[index] = null;
    }

    private void initNvarcLenColumns(int index, int size) {
        nvarc_len_columns[index] = ByteBuffer.allocateDirect(4 * size).order(ByteOrder.LITTLE_ENDIAN);
    }

    public void resetNvarcLenColumns(int index) {
        nvarc_len_columns[index] = null;
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
            data_columns[idx].clear();
        }
    }

    public int getTotalLengthForHeader(int row_length, int row_counter) {
        int total_bytes = 0;
        for(int idx=0; idx < row_length; idx++) {
            total_bytes += (null_columns[idx] != null) ? row_counter : 0;
            total_bytes += (nvarc_len_columns[idx] != null) ? 4 * row_counter : 0;
            total_bytes += data_columns[idx].position();
        }
        return total_bytes;
    }

    public void setNullColumns(int index, ByteBuffer value) {
        null_columns[index] = value;
    }

    public void setNvarcLenColumns(int index, ByteBuffer value) {
        nvarc_len_columns[index] = value;
    }

    public void setDataColumns(int index, ByteBuffer value) {
        data_columns[index] = value;
    }

    public ByteBuffer[] getDataColumns() {
        return data_columns;
    }

    public ByteBuffer[] getNullColumns() {
        return null_columns;
    }

    public ByteBuffer[] getNvarcLenColumns() {
        return nvarc_len_columns;
    }

    public ByteBuffer getDataColumns(int index) {
        return data_columns[index];
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
}
