package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.TableMetadata;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.BitSet;

import static com.sqream.jdbc.utils.Utils.*;

public class FlushStorage {
    private TableMetadata metadata;
    private BlockDto curBlock;
    private BitSet columns_set;
    private RowIterator rowIterator;

    public FlushStorage(TableMetadata metadata, BlockDto block) {
        this.metadata = metadata;
        this.curBlock = block;
        columns_set = new BitSet(metadata.getRowLength());
        initIterator(block);
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

    public void setBlock(BlockDto block) {
        curBlock = block;
        rowIterator = new RowIterator(block.getCapacity());
        columns_set = new BitSet(metadata.getRowLength());
        initIterator(block);
    }

    public BlockDto getBlock() {
        curBlock.setFillSize(rowIterator.getRowIndex());
        return curBlock;
    }

    public int getTotalLengthForHeader(int row_length, int row_counter) {
        int total_bytes = 0;
        for(int idx=0; idx < row_length; idx++) {
            total_bytes += (curBlock.getNullBuffers()[idx] != null) ? row_counter : 0;
            total_bytes += (curBlock.getNvarcLenBuffers()[idx] != null) ? 4 * row_counter : 0;
            total_bytes += curBlock.getDataBuffers()[idx].position();
        }
        return total_bytes;
    }


    public void setBoolean(int colIndex, Boolean value) {
        if (value != null) {
            curBlock.getDataBuffers()[colIndex].put((byte) ((value) ? 1 : 0));
        } else {
            curBlock.getDataBuffers()[colIndex].put((byte) 0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setUbyte(int colIndex, Byte value) {
        if (value != null) {
            curBlock.getDataBuffers()[colIndex].put(value);
        } else {
            curBlock.getDataBuffers()[colIndex].put((byte) 0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setShort(int colIndex, Short value) {
        if (value != null) {
            curBlock.getDataBuffers()[colIndex].putShort(value);
        } else {
            curBlock.getDataBuffers()[colIndex].putShort((short) 0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setInt(int colIndex, Integer value) {
        if (value != null) {
            curBlock.getDataBuffers()[colIndex].putInt(value);
        } else {
            curBlock.getDataBuffers()[colIndex].putInt(0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setLong(int colIndex, Long value) {
        if (value != null) {
            curBlock.getDataBuffers()[colIndex].putLong(value);
        } else {
            curBlock.getDataBuffers()[colIndex].putLong(0L);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setFloat(int colIndex, Float value) {
        if (value != null) {
            curBlock.getDataBuffers()[colIndex].putFloat(value);
        } else {
            curBlock.getDataBuffers()[colIndex].putFloat(0f);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setDouble(int colIndex, Double value) {
        if (value != null) {
            curBlock.getDataBuffers()[colIndex].putDouble(value);
        } else {
            curBlock.getDataBuffers()[colIndex].putDouble(0d);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setVarchar(int colIndex, byte[] stringBytes, String originalString) {
        // Generate missing spaces to fill up to size
        byte [] spaces = new byte[metadata.getSize(colIndex) - stringBytes.length];
        Arrays.fill(spaces, (byte) 32);  // ascii value of space
        // Set value and added spaces if needed
        curBlock.getDataBuffers()[colIndex].put(stringBytes);
        curBlock.getDataBuffers()[colIndex].put(spaces);

        if (originalString == null) {
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setNvarchar(int colIndex, byte[] stringBytes, String originalString) {
        // Add string length to lengths column
        curBlock.getNvarcLenBuffers()[colIndex].putInt(stringBytes.length);
        // Set actual value
        if (stringBytes.length > curBlock.getDataBuffers()[colIndex].remaining()) {
            increaseBuffer(colIndex, stringBytes.length);
        }
        curBlock.getDataBuffers()[colIndex].put(stringBytes);

        if (originalString == null) {
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setDate(int colIndex, Date date, ZoneId zone) {
        if (date != null) {
            curBlock.getDataBuffers()[colIndex].putInt(dateToInt(date, zone));
        } else {
            curBlock.getDataBuffers()[colIndex].putInt(0);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setDatetime(int colIndex, Timestamp timestamp, ZoneId zone) {
        if (timestamp != null) {
            curBlock.getDataBuffers()[colIndex].putLong(dtToLong(timestamp, zone));
        } else {
            curBlock.getDataBuffers()[colIndex].putLong(0L);
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    private void initIterator(BlockDto block) {
        rowIterator = new RowIterator(block.getCapacity());
        rowIterator.next();
    }

    private void markAsNull(int index) {
        curBlock.getNullBuffers()[index].put((byte) 1);
    }

    private void increaseBuffer(int index, int puttingStringLength) {
        ByteBuffer new_text_buf = ByteBuffer.allocateDirect((curBlock.getDataBuffers()[index].capacity() + puttingStringLength) * 2)
                .order(ByteOrder.LITTLE_ENDIAN);
        new_text_buf.put(curBlock.getDataBuffers()[index]);
        curBlock.getDataBuffers()[index] = new_text_buf;
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
