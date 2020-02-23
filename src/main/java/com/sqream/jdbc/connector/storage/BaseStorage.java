package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.MemoryAllocationService;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.byteReaders.ByteReaderFactory;

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

public abstract class BaseStorage implements Storage {
    private static final Logger LOGGER = Logger.getLogger(BaseStorage.class.getName());

    protected ByteBuffer[] dataColumns;
    protected ByteBuffer[] nullColumns;
    protected ByteBuffer[] nvarcLenColumns;
    protected int curBlockCapacity = 0;
    protected TableMetadata metadata;
    protected RowIterator rowIterator;

    BaseStorage(TableMetadata metadata, BlockDto block) {
        this.metadata = metadata;
        setBlock(block);
    }

    public boolean next() throws ConnException {
        return rowIterator.next();
    }

    public void clearBuffers(int row_length) {
        for(int idx=0; idx < row_length; idx++) {
            if (nullColumns[idx] != null) {
                // Clear doesn't actually nullify/reset the data
                nullColumns[idx].clear();
                //TODO: Alex K 13.01.2020 Check why previously allocated DirectByteBuffer and reset with HeapByteBuffer
                nullColumns[idx].put(ByteBuffer.allocate(curBlockCapacity));
                nullColumns[idx].clear();
            }
            if(nvarcLenColumns[idx] != null)
                nvarcLenColumns[idx].clear();
            dataColumns[idx].clear();
        }
        rowIterator = new RowIterator(curBlockCapacity);
        rowIterator.next();
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
        curBlockCapacity = block.getCapacity();
    }

    public BlockDto getBlock() {
        throw new UnsupportedOperationException();
    }

    public void setBoolean(int colIndex, Boolean value) {
        throw new UnsupportedOperationException();
    }

    public void setUbyte(int colIndex, Byte value) {
        throw new UnsupportedOperationException();
    }

    public void setShort(int colIndex, Short value) {
        throw new UnsupportedOperationException();
    }

    public void setInt(int colIndex, Integer value) {
        throw new UnsupportedOperationException();
    }

    public void setLong(int colIndex, Long value) {
        throw new UnsupportedOperationException();
    }

    public void setFloat(int colIndex, Float value) {
        throw new UnsupportedOperationException();
    }

    public void setDouble(int colIndex, Double value) {
        throw new UnsupportedOperationException();
    }

    public void setVarchar(int colIndex, byte[] stringBytes, String originalString) {
        throw new UnsupportedOperationException();
    }

    public void setNvarchar(int colIndex, byte[] stringBytes, String originalString) {
        throw new UnsupportedOperationException();
    }

    public void setDate(int colIndex, Date date, ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    public void setDatetime(int colIndex, Timestamp timestamp, ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    public Boolean getBoolean(int colIndex) {
        throw new UnsupportedOperationException();
    }

    public Byte getUbyte(int colIndex) {
        throw new UnsupportedOperationException();
    }

    public Short getShort(int colIndex) {
        throw new UnsupportedOperationException();
    }

    public Integer getInt(int colIndex) {
        throw new UnsupportedOperationException();
    }

    public Long getLong(int colIndex) {
        throw new UnsupportedOperationException();
    }

    public Float getFloat(int colIndex) {
        throw new UnsupportedOperationException();
    }

    public Double getDouble(int colIndex) {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int colIndex, ZoneId zoneId) {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(int colIndex, ZoneId zoneId) {
        throw new UnsupportedOperationException();
    }

    public String getVarchar(int colIndex, String varcharEncoding) {
        throw new UnsupportedOperationException();
    }

    public String getNvarchar(int colIndex, Charset varcharEncoding) {
        throw new UnsupportedOperationException();
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
