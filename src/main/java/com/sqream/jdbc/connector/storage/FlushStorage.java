package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.byteWriters.ByteWriterFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.BitSet;

import static com.sqream.jdbc.utils.Utils.*;

public class FlushStorage {
    private static final int MAX_BUFFER_SIZE = 500_000_000;

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
        return rowIterator.next() && !curBlock.isLimitReached();
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

    public void setBoolean(int colIndex, Boolean value) {
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeBoolean(curBlock.getDataBuffers()[colIndex], value != null && value ? (byte) 1 : 0);
        if (value == null) {
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setUbyte(int colIndex, Byte value) {
        if (value == null) {
            markAsNull(colIndex);
            value = (byte) 0;
        }
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeUbyte(curBlock.getDataBuffers()[colIndex], value);
        columns_set.set(colIndex);
    }

    public void setShort(int colIndex, Short value) {
        if (value == null) {
            markAsNull(colIndex);
            value = 0;
        }
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeShort(curBlock.getDataBuffers()[colIndex], value);
        columns_set.set(colIndex);
    }

    public void setInt(int colIndex, Integer value) {
        if (value == null) {
            value = 0;
            markAsNull(colIndex);
        }
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeInt(curBlock.getDataBuffers()[colIndex], value);
        columns_set.set(colIndex);
    }

    public void setLong(int colIndex, Long value) {
        if (value == null) {
            value = 0L;
            markAsNull(colIndex);
        }
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeLong(curBlock.getDataBuffers()[colIndex], value);
        columns_set.set(colIndex);
    }

    public void setFloat(int colIndex, Float value) {
        if (value == null) {
            value = 0f;
            markAsNull(colIndex);
        }
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeFloat(curBlock.getDataBuffers()[colIndex], value);
        columns_set.set(colIndex);
    }

    public void setDouble(int colIndex, Double value) {
        if (value == null) {
            value = 0d;
            markAsNull(colIndex);
        }
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeDouble(curBlock.getDataBuffers()[colIndex], value);
        columns_set.set(colIndex);
    }

    public void setVarchar(int colIndex, byte[] stringBytes, String originalString) {
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeVarchar(curBlock.getDataBuffers()[colIndex], stringBytes, metadata.getSize(colIndex));

        if (originalString == null) {
            markAsNull(colIndex);
        }
        columns_set.set(colIndex);
    }

    public void setNvarchar(int colIndex, byte[] stringBytes, String originalString) throws ConnException {
        // Add string length to lengths column
        curBlock.getNvarcLenBuffers()[colIndex].putInt(stringBytes.length);
        // Set actual value
        if (stringBytes.length > curBlock.getDataBuffers()[colIndex].remaining()) {
            increaseBuffer(colIndex, stringBytes.length);
        }
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeNvarchar(curBlock.getDataBuffers()[colIndex], stringBytes);

        if (originalString == null) {
            markAsNull(colIndex);
        }
        if (curBlock.getDataBuffers()[colIndex].position() > MAX_BUFFER_SIZE) {
            curBlock.setLimitReached(true);
        }
        columns_set.set(colIndex);
    }

    public void setDate(int colIndex, Date date, ZoneId zone) {
        if (date == null) {
            markAsNull(colIndex);
        }
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeDate(curBlock.getDataBuffers()[colIndex], date == null ? 0 : dateToInt(date, zone));
        columns_set.set(colIndex);
    }

    public void setDatetime(int colIndex, Timestamp timestamp, ZoneId zone) {
        if (timestamp == null) {
            markAsNull(colIndex);
        }
        ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeDateTime(curBlock.getDataBuffers()[colIndex], timestamp == null ? 0 : dtToLong(timestamp, zone));
        columns_set.set(colIndex);
    }

    private void initIterator(BlockDto block) {
        rowIterator = new RowIterator(block.getCapacity());
        rowIterator.next();
    }

    private void markAsNull(int index) {
        curBlock.getNullBuffers()[index].put((byte) 1);
    }

    private void increaseBuffer(int index, int puttingStringLength) throws ConnException {
        int oldSize = curBlock.getDataBuffers()[index].capacity();
        int newSize;
        if (oldSize >= MAX_BUFFER_SIZE) {
            curBlock.setLimitReached(true);
        }
        try {
            newSize = Math.multiplyExact(Math.addExact(oldSize, puttingStringLength), 2);
        } catch (ArithmeticException e) {
            if (Integer.MAX_VALUE - oldSize > puttingStringLength) {
                newSize = Integer.MAX_VALUE;
            } else {
                throw new ConnException(MessageFormat.format("Data buffer size exceeds maximum size supported", Integer.MAX_VALUE));
            }
        }
        ByteBuffer newTextBuf = ByteBuffer.allocateDirect(newSize)
                .order(ByteOrder.LITTLE_ENDIAN);

        final ByteBuffer readOnlyCopy = curBlock.getDataBuffers()[index];

        readOnlyCopy.flip();
        newTextBuf.put(readOnlyCopy);

        newTextBuf.position(readOnlyCopy.position());

        newTextBuf.put(curBlock.getDataBuffers()[index]);
        curBlock.getDataBuffers()[index] = newTextBuf;
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
