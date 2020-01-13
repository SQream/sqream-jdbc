package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.byteReaders.ByteReaderFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

import static com.sqream.jdbc.utils.Utils.*;

public class ColumnStorage {

    private ByteBuffer[] dataColumns;
    private ByteBuffer[] null_columns;
    private ByteBuffer[] nvarc_len_columns;
    private TableMetadata metadata;
    private int blockSize;

    ColumnStorage() { }

    public static BuilderWithMetadata builder() {
        return new ColumnStorageBuilder();
    }

    void init(TableMetadata metadata, int blockSize) {
        initArrays(metadata, blockSize);
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

    void initFromFetch(ByteBuffer[] fetchBuffers, TableMetadata metadata, int blockSize) {
        initArrays(metadata, blockSize);
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

    private void initArrays(TableMetadata metadata, int blockSize) {
        this.metadata = metadata;
        this.blockSize = blockSize;
        dataColumns = new ByteBuffer[metadata.getRowLength()];
        null_columns = new ByteBuffer[metadata.getRowLength()];
        nvarc_len_columns = new ByteBuffer[metadata.getRowLength()];
    }

    public void loadBlock(BlockDto block) {
        this.dataColumns = block.getDataBuffers();
        this.null_columns = block.getNullBuffers();
        this.nvarc_len_columns = block.getNvarcLenBuffers();
    }

    public void clearBuffers(int row_length) {
        for(int idx=0; idx < row_length; idx++) {
            if (null_columns[idx] != null) {
                // Clear doesn't actually nullify/reset the data
                null_columns[idx].clear();
                //TODO: Alex K 13.01.2020 Check why previously allocated DirectByteBuffer and reset with HeapByteBuffer
                null_columns[idx].put(ByteBuffer.allocate(blockSize));
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

    public void setBlock(BlockDto block) {
        dataColumns = block.getDataBuffers();
        null_columns = block.getNullBuffers();
        nvarc_len_columns = block.getNvarcLenBuffers();
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

    public boolean isNotNull(int colIndex, int rowIndex) {
        return null_columns[colIndex] == null || null_columns[colIndex].get(rowIndex) == 0;
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

    public void setNvarchar(int index, byte[] stringBytes, String originalString) {
        // Add string length to lengths column
        nvarc_len_columns[index].putInt(stringBytes.length);
        // Set actual value
        if (stringBytes.length > dataColumns[index].remaining()) {
            increaseBuffer(index, stringBytes.length);
        }
        dataColumns[index].put(stringBytes);

        if (originalString == null) {
            markAsNull(index);
        }
    }

    public void setDate(int index, Date date, ZoneId zone) {
        if (date != null) {
            dataColumns[index].putInt(dateToInt(date, zone));
        } else {
            dataColumns[index].putInt(0);
            markAsNull(index);
        }
    }

    public void setDatetime(int index, Timestamp timestamp, ZoneId zone) {
        if (timestamp != null) {
            dataColumns[index].putLong(dtToLong(timestamp, zone));
        } else {
            dataColumns[index].putLong(0L);
            markAsNull(index);
        }
    }

    public Boolean getBoolean(int colIndex, int rowIndex) {
        return isNotNull(colIndex, rowIndex) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readBoolean(dataColumns[colIndex], rowIndex) : null;
    }

    public Byte getUbyte(int colIndex, int rowIndex) {
        return isNotNull(colIndex, rowIndex) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readUbyte(dataColumns[colIndex], rowIndex) : null;
    }

    public Short getShort(int colIndex, int rowIndex) {
        return isNotNull(colIndex, rowIndex) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readShort(dataColumns[colIndex], rowIndex) : null;
    }

    public Integer getInt(int colIndex, int rowIndex) {
        return isNotNull(colIndex, rowIndex) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readInt(dataColumns[colIndex], rowIndex) : null;
    }

    public Long getLong(int colIndex, int rowIndex) {
        return isNotNull(colIndex, rowIndex) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readLong(dataColumns[colIndex], rowIndex) : null;
    }

    public Float getFloat(int colIndex, int rowIndex) {
        return isNotNull(colIndex, rowIndex) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readFloat(dataColumns[colIndex], rowIndex) : null;
    }

    public Double getDouble(int colIndex, int rowIndex) {
        return isNotNull(colIndex, rowIndex) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readDouble(dataColumns[colIndex], rowIndex) : null;
    }

    public Date getDate(int colIndex, int rowIndex, ZoneId zoneId) {
        if (!isNotNull(colIndex, rowIndex)) {
            return null;
        }

        int dateAsInt = ByteReaderFactory
                .getReader(metadata.getType(colIndex))
                .readDate(dataColumns[colIndex], rowIndex);

        return intToDate(dateAsInt, zoneId);
    }

    public Timestamp getTimestamp(int colIndex, int rowIndex, ZoneId zoneId) {
        if (!isNotNull(colIndex, rowIndex)) {
            return null;
        }

        long dateTimeAsLong = ByteReaderFactory
                .getReader(metadata.getType(colIndex))
                .readDateTime(dataColumns[colIndex], rowIndex);

        return longToDt(dateTimeAsLong, zoneId);
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

    private void markAsNull(int index) {
        null_columns[index].put((byte) 1);
    }

    private void increaseBuffer(int index, int puttingStringLength) {
        ByteBuffer new_text_buf = ByteBuffer.allocateDirect((dataColumns[index].capacity() + puttingStringLength) * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        new_text_buf.put(dataColumns[index]);
        dataColumns[index] = new_text_buf;
    }

    private static long dtToLong(Timestamp ts, ZoneId zone) {  // ZonedDateTime

        if (ts == null)
            return 0;

        LocalDateTime datetime = ts.toInstant().atZone(zone).toLocalDateTime();

        //LocalDateTime datetime = ts.toLocalDateTime();
        int year  = datetime.getYear();
        int month = datetime.getMonthValue();
        int day   = datetime.getDayOfMonth();

        month = (month + 9) % 12;
        year = year - month / 10;

        int date_as_int = (365 * year + year / 4 - year / 100 + year / 400 + (month * 306 + 5) / 10 + (day - 1));

        int time_as_int =  datetime.getHour() * 3600000;
        time_as_int += datetime.getMinute() * 60000;
        time_as_int += datetime.getSecond() * 1000;
        time_as_int += datetime.getNano() / 1000000;


        return (((long) date_as_int) << 32) | (time_as_int & 0xffffffffL);
    }
}
