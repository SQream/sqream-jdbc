package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.byteReaders.ByteReaderFactory;

import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;

import static com.sqream.jdbc.utils.Utils.intToDate;
import static com.sqream.jdbc.utils.Utils.longToDt;

public class FetchStorage extends BaseStorage implements Storage {

    public FetchStorage(TableMetadata metadata, BlockDto block) {
        super(metadata, block);
    }

    public boolean next() {
        return rowIterator.next();
    }

    @Override
    public void setBlock(BlockDto block) {
        rowIterator = new RowIterator(block.getFillSize());
        super.setBlock(block);
    }

    @Override
    public Boolean getBoolean(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readBoolean(dataColumns[colIndex], rowIterator.getRowIndex()) : null;
    }

    @Override
    public Byte getUbyte(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readUbyte(dataColumns[colIndex], rowIterator.getRowIndex()) : null;
    }

    @Override
    public Short getShort(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readShort(dataColumns[colIndex], rowIterator.getRowIndex()) : null;
    }

    @Override
    public Integer getInt(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readInt(dataColumns[colIndex], rowIterator.getRowIndex()) : null;
    }

    @Override
    public Long getLong(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readLong(dataColumns[colIndex], rowIterator.getRowIndex()) : null;
    }

    @Override
    public Float getFloat(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readFloat(dataColumns[colIndex], rowIterator.getRowIndex()) : null;
    }

    @Override
    public Double getDouble(int colIndex) {
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readDouble(dataColumns[colIndex], rowIterator.getRowIndex()) : null;
    }

    @Override
    public Date getDate(int colIndex, ZoneId zoneId) {
        if (!isNotNull(colIndex, rowIterator.getRowIndex())) {
            return null;
        }

        int dateAsInt = ByteReaderFactory
                .getReader(metadata.getType(colIndex))
                .readDate(dataColumns[colIndex], rowIterator.getRowIndex());

        return intToDate(dateAsInt, zoneId);
    }

    @Override
    public Timestamp getTimestamp(int colIndex, ZoneId zoneId) {
        if (!isNotNull(colIndex, rowIterator.getRowIndex())) {
            return null;
        }

        long dateTimeAsLong = ByteReaderFactory
                .getReader(metadata.getType(colIndex))
                .readDateTime(dataColumns[colIndex], rowIterator.getRowIndex());

        return longToDt(dateTimeAsLong, zoneId);
    }

    @Override
    public String getVarchar(int colIndex, String varcharEncoding, boolean repeatedly) {
        int colSize = metadata.getSize(colIndex);
        if (repeatedly) {
            dataColumns[colIndex].position(dataColumns[colIndex].position() - colSize);
        }
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readVarchar(dataColumns[colIndex], colSize, varcharEncoding) : null;
    }

    @Override
    public String getNvarchar(int colIndex, Charset varcharEncoding, boolean repeatedly) {
        int nvarcLen = nvarcLenColumns[colIndex].getInt(rowIterator.getRowIndex() * 4);
        if (repeatedly) {
            dataColumns[colIndex].position(dataColumns[colIndex].position() - nvarcLen);
        }
        return isNotNull(colIndex, rowIterator.getRowIndex()) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readNvarchar(dataColumns[colIndex], nvarcLen, varcharEncoding) : null;
    }

    private boolean isNotNull(int colIndex, int rowIndex) {
        return nullColumns[colIndex] == null || nullColumns[colIndex].get(rowIndex) == 0;
    }


}
