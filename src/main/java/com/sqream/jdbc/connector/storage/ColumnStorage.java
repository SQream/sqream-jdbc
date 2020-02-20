package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.MemoryAllocationService;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.byteReaders.ByteReaderFactory;
import com.sqream.jdbc.connector.storage.storageIterator.InsertRowIterator;
import com.sqream.jdbc.connector.storage.storageIterator.RowIterator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.sqream.jdbc.utils.Utils.*;

public class ColumnStorage {
    private static final Logger LOGGER = Logger.getLogger(ColumnStorage.class.getName());

    private ByteBuffer[] dataColumns;
    private ByteBuffer[] nullColumns;
    private ByteBuffer[] nvarcLenColumns;
    private int blockCapacity;
    private TableMetadata metadata;
    private int blockSize;
    private BitSet columns_set;
    private RowIterator rowIterator;

    ColumnStorage() { }

    public static BuilderWithMetadata builder() {
        return new ColumnStorageBuilder();
    }

    void init(TableMetadata metadata, int blockSize) {
        LOGGER.log(Level.FINE, MessageFormat.format(
                "Start to init storage for block size = [{0}}", blockSize));

        BlockDto block = new MemoryAllocationService().buildBlock(metadata, blockSize);
        this.metadata = metadata;
        this.blockSize = blockSize;
        this.dataColumns = block.getDataBuffers();
        this.nullColumns = block.getNullBuffers();
        this.nvarcLenColumns = block.getNvarcLenBuffers();
        this.rowIterator = new InsertRowIterator(blockSize);
        resetCounters();

        if (LOGGER.getParent().getLevel() == Level.FINE) {
            LOGGER.log(Level.FINE, MessageFormat.format("Initialized block. Allocated [{0}] mb.",
                    calculateAllocation(dataColumns, nullColumns, nvarcLenColumns) / 1_000_000));
        }
    }

    private void resetCounters() {
        this.columns_set = new BitSet(metadata.getRowLength());
        this.rowIterator.reset();
    }

    void initFromFetch(ByteBuffer[] fetchBuffers, TableMetadata metadata, int blockSize) {
        init(metadata, blockSize);
        copyFromFetchedBuffers(fetchBuffers);
    }

    public boolean next() throws ConnException {
        if (columns_set.cardinality() < metadata.getRowLength()) {
            throw new ConnException(MessageFormat.format(
                    "All columns must be set before calling next(). Set [{0}] columns out of [{1}]",
                    columns_set.cardinality(), metadata.getRowLength()));
        }
        columns_set.clear();
        return rowIterator.next();
    }

    private void copyFromFetchedBuffers(ByteBuffer[] fetchBuffers) {
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
    }

    public void clearBuffers(int row_length) {
        for(int idx=0; idx < row_length; idx++) {
            if (nullColumns[idx] != null) {
                // Clear doesn't actually nullify/reset the data
                nullColumns[idx].clear();
                //TODO: Alex K 13.01.2020 Check why previously allocated DirectByteBuffer and reset with HeapByteBuffer
                nullColumns[idx].put(ByteBuffer.allocate(blockSize));
                nullColumns[idx].clear();
            }
            if(nvarcLenColumns[idx] != null)
                nvarcLenColumns[idx].clear();
            dataColumns[idx].clear();
        }
    }

    public int getTotalLengthForHeader(int row_length, int row_counter) {
        int total_bytes = 0;
        for(int idx=0; idx < row_length; idx++) {
            total_bytes += (nullColumns[idx] != null) ? row_counter : 0;
            total_bytes += (nvarcLenColumns[idx] != null) ? 4 * row_counter : 0;
            total_bytes += dataColumns[idx].position();
        }
        return total_bytes;
    }

    public void setBlock(BlockDto block) {
        dataColumns = block.getDataBuffers();
        nullColumns = block.getNullBuffers();
        nvarcLenColumns = block.getNvarcLenBuffers();
        blockCapacity = block.getCapacity();
        resetCounters();
    }

    public BlockDto getBlock() {
        return new BlockDto(dataColumns, nullColumns, nvarcLenColumns, blockCapacity);
    }

    public boolean isNotNull(int colIndex, int rowIndex) {
        return nullColumns[colIndex] == null || nullColumns[colIndex].get(rowIndex) == 0;
    }

    public void setBoolean(int colIndex, Boolean value) {
        if (value != null) {
            dataColumns[colIndex].put((byte) ((value) ? 1 : 0));
        } else {
            dataColumns[colIndex].put((byte) 0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setUbyte(int colIndex, Byte value) {
        if (value != null) {
            dataColumns[colIndex].put(value);
        } else {
            dataColumns[colIndex].put((byte) 0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setShort(int colIndex, Short value) {
        if (value != null) {
            dataColumns[colIndex].putShort(value);
        } else {
            dataColumns[colIndex].putShort((short) 0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setInt(int colIndex, Integer value) {
        if (value != null) {
            dataColumns[colIndex].putInt(value);
        } else {
            dataColumns[colIndex].putInt(0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setLong(int colIndex, Long value) {
        if (value != null) {
            dataColumns[colIndex].putLong(value);
        } else {
            dataColumns[colIndex].putLong(0L);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setFloat(int colIndex, Float value) {
        if (value != null) {
            dataColumns[colIndex].putFloat(value);
        } else {
            dataColumns[colIndex].putFloat(0f);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setDouble(int colIndex, Double value) {
        if (value != null) {
            dataColumns[colIndex].putDouble(value);
        } else {
            dataColumns[colIndex].putDouble(0d);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setVarchar(int colIndex, byte[] stringBytes, String originalString) {
        // Generate missing spaces to fill up to size
        byte [] spaces = new byte[metadata.getSize(colIndex) - stringBytes.length];
        Arrays.fill(spaces, (byte) 32);  // ascii value of space
        // Set value and added spaces if needed
        dataColumns[colIndex].put(stringBytes);
        dataColumns[colIndex].put(spaces);

        if (originalString == null) {
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setNvarchar(int colIndex, byte[] stringBytes, String originalString) {
        // Add string length to lengths column
        nvarcLenColumns[colIndex].putInt(stringBytes.length);
        // Set actual value
        if (stringBytes.length > dataColumns[colIndex].remaining()) {
            increaseBuffer(colIndex, stringBytes.length);
        }
        dataColumns[colIndex].put(stringBytes);

        if (originalString == null) {
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setDate(int colIndex, Date date, ZoneId zone) {
        if (date != null) {
            dataColumns[colIndex].putInt(dateToInt(date, zone));
        } else {
            dataColumns[colIndex].putInt(0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setDatetime(int colIndex, Timestamp timestamp, ZoneId zone) {
        if (timestamp != null) {
            dataColumns[colIndex].putLong(dtToLong(timestamp, zone));
        } else {
            dataColumns[colIndex].putLong(0L);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
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

    public String getVarchar(int colIndex, int rowIndex, String varcharEncoding, boolean repeatedly) {
        int colSize = metadata.getSize(colIndex);
        if (repeatedly) {
            dataColumns[colIndex].position(dataColumns[colIndex].position() - colSize);
        }
        return isNotNull(colIndex, rowIndex) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readVarchar(dataColumns[colIndex], colSize, varcharEncoding) : null;
    }

    public String getNvarchar(int colIndex, int rowIndex, Charset varcharEncoding, boolean repeatedly) {
        int nvarcLen = nvarcLenColumns[colIndex].getInt(rowIndex * 4);
        if (repeatedly) {
            dataColumns[colIndex].position(dataColumns[colIndex].position() - nvarcLen);
        }
        return isNotNull(colIndex, rowIndex) ?
                ByteReaderFactory
                        .getReader(metadata.getType(colIndex))
                        .readNvarchar(dataColumns[colIndex], nvarcLen, varcharEncoding) : null;
    }

    private void markAsNull(int index) {
        nullColumns[index].put((byte) 1);
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
