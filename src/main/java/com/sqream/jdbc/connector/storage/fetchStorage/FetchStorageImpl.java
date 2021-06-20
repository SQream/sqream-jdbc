package com.sqream.jdbc.connector.storage.fetchStorage;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.byteReaders.ByteReaderFactory;
import com.sqream.jdbc.connector.storage.RowIterator;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.BitSet;

import static com.sqream.jdbc.utils.Utils.intToDate;
import static com.sqream.jdbc.utils.Utils.longToDt;

public class FetchStorageImpl implements FetchStorage {
    private TableMetadata metadata;
    private BlockDto curBlock;
    private RowIterator rowIterator;
    private int[] col_calls;

    public FetchStorageImpl(TableMetadata metadata, BlockDto block) {
        this.metadata = metadata;
        this.curBlock = block;
        col_calls = new int[metadata.getRowLength()];
        initIterator(block);
    }

    public boolean next() {
        Arrays.fill(col_calls, 0); // calls in the same fetch - for varchar / nvarchar
        return rowIterator.next();
    }

    public void setBlock(BlockDto block) {
        curBlock = block;
        initIterator(block);
        rowIterator.next();
    }

    public Boolean getBoolean(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readBoolean(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex()) : null;
    }

    public Byte getUbyte(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readUbyte(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex()) : null;
    }

    public Short getShort(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readShort(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex()) : null;
    }

    public Integer getInt(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readInt(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex()) : null;
    }

    public Long getLong(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readLong(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex()) : null;
    }

    public Float getFloat(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readFloat(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex()) : null;
    }

    public Double getDouble(int colIndex) {
        if (metadata.getType(colIndex).equals("ftNumeric")) {
            return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                    .getReader(metadata.getType(colIndex))
                    .readDouble(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex(), metadata.getScale(colIndex)) : null;
        }
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readDouble(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex()) : null;
    }

    public BigDecimal getBigDecimal(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readBigDecimal(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex(), metadata.getScale(colIndex)) : null;
    }

    public Date getDate(int colIndex, ZoneId zoneId) {
        if (!isNotNull(colIndex, rowIterator.getRowIndex())) {
            return null;
        }

        int dateAsInt = ByteReaderFactory
                .getReader(metadata.getType(colIndex))
                .readDate(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex());

        return intToDate(dateAsInt, zoneId);
    }

    public Timestamp getTimestamp(int colIndex, ZoneId zoneId) {
        if (!isNotNull(colIndex, rowIterator.getRowIndex())) {
            return null;
        }

        long dateTimeAsLong = ByteReaderFactory
                .getReader(metadata.getType(colIndex))
                .readDateTime(curBlock.getDataBuffers()[colIndex], rowIterator.getRowIndex());

        return longToDt(dateTimeAsLong, zoneId);
    }

    public String getVarchar(int colIndex, String varcharEncoding) {
        int colSize = metadata.getSize(colIndex);
        boolean repeatedly = col_calls[colIndex]++ > 0;
        if (repeatedly) {
            curBlock.getDataBuffers()[colIndex].position(curBlock.getDataBuffers()[colIndex].position() - colSize);
        }
        String result = ByteReaderFactory
                .getReader(metadata.getType(colIndex))
                .readVarchar(curBlock.getDataBuffers()[colIndex], colSize, varcharEncoding);
        return isNotNull(colIndex, rowIterator.getRowIndex()) ? result : null;
    }

    public String getNvarchar(int colIndex, Charset varcharEncoding) {
        int nvarcLen = curBlock.getNvarcLenBuffers()[colIndex].getInt(rowIterator.getRowIndex() * 4);
        boolean repeatedly = col_calls[colIndex]++ > 0;
        if (repeatedly) {
            curBlock.getDataBuffers()[colIndex].position(curBlock.getDataBuffers()[colIndex].position() - nvarcLen);
        }
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readNvarchar(curBlock.getDataBuffers()[colIndex], nvarcLen, varcharEncoding) : null;
    }

    private void initIterator(BlockDto block) {
        this.rowIterator = new RowIterator(block.getFillSize());
    }

    private boolean isNotNull(int colIndex, int rowIndex) {
        return curBlock.getNullBuffers()[colIndex] == null || curBlock.getNullBuffers()[colIndex].get(rowIndex) == 0;
    }


}
