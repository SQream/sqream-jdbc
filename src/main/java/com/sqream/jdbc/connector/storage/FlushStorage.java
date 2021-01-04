package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.BlockDto;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.TableMetadata;
import com.sqream.jdbc.connector.byteWriters.ByteWriterFactory;

import java.math.BigDecimal;
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
    private TableMetadata metadata;
    private BlockDto curBlock;
    private BitSet columnsSet;
    private RowIterator rowIterator;
    private long memoryLimit;
    private long blockFullness = 0;

    public FlushStorage(TableMetadata metadata, BlockDto block, long memoryLimit) {
        this.metadata = metadata;
        this.curBlock = block;
        this.memoryLimit = memoryLimit;
        this.columnsSet = new BitSet(metadata.getRowLength());
        initIterator(block);
    }

    public boolean next() throws ConnException {
        if (columnsSet.cardinality() < metadata.getRowLength()) {
            throw new ConnException(MessageFormat.format(
                    "All columns must be set before calling next(). Set [{0}] columns out of [{1}]",
                    columnsSet.cardinality(), metadata.getRowLength()));
        }
        columnsSet.clear();
        return rowIterator.next() && !memoryLimitReached();
    }

    public void setBlock(BlockDto block) {
        curBlock = block;
        rowIterator = new RowIterator(block.getCapacity());
        columnsSet = new BitSet(metadata.getRowLength());
        initIterator(block);
        blockFullness = 0;
    }

    public BlockDto getBlock() {
        curBlock.setFillSize(rowIterator.getRowIndex());
        return curBlock;
    }

    public void setBoolean(int colIndex, Boolean value) {
        if (value == null) {
            markAsNull(colIndex);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeBoolean(curBlock.getDataBuffers()[colIndex], value != null && value ? (byte) 1 : 0);
        columnsSet.set(colIndex);
    }

    public void setUbyte(int colIndex, Byte value) {
        if (value == null) {
            markAsNull(colIndex);
            value = (byte) 0;
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeUbyte(curBlock.getDataBuffers()[colIndex], value);
        columnsSet.set(colIndex);
    }

    public void setShort(int colIndex, Short value) {
        if (value == null) {
            markAsNull(colIndex);
            value = 0;
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeShort(curBlock.getDataBuffers()[colIndex], value);
        columnsSet.set(colIndex);
    }

    public void setInt(int colIndex, Integer value) {
        if (value == null) {
            value = 0;
            markAsNull(colIndex);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeInt(curBlock.getDataBuffers()[colIndex], value);
        columnsSet.set(colIndex);
    }

    public void setLong(int colIndex, Long value) {
        if (value == null) {
            value = 0L;
            markAsNull(colIndex);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeLong(curBlock.getDataBuffers()[colIndex], value);
        columnsSet.set(colIndex);
    }

    public void setFloat(int colIndex, Float value) {
        if (value == null) {
            value = 0f;
            markAsNull(colIndex);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeFloat(curBlock.getDataBuffers()[colIndex], value);
        columnsSet.set(colIndex);
    }

    public void setDouble(int colIndex, Double value) {
        if (value == null) {
            value = 0d;
            markAsNull(colIndex);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeDouble(curBlock.getDataBuffers()[colIndex], value);
        columnsSet.set(colIndex);
    }

    public void setBigDecimal(int colIndex, BigDecimal value) {
        if (value == null) {
            markAsNull(colIndex);
            value = new BigDecimal(0);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeNumeric(curBlock.getDataBuffers()[colIndex], value, metadata.getScale(colIndex));
        columnsSet.set(colIndex);
    }

    public void setVarchar(int colIndex, byte[] stringBytes, String originalString) {
        if (originalString == null) {
            markAsNull(colIndex);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeVarchar(curBlock.getDataBuffers()[colIndex], stringBytes, metadata.getSize(colIndex));
        columnsSet.set(colIndex);
    }

    public void setNvarchar(int colIndex, byte[] stringBytes, String originalString) throws ConnException {
        // Add string length to lengths column
        curBlock.getNvarcLenBuffers()[colIndex].putInt(stringBytes.length);
        blockFullness++;
        // Set actual value
        if (stringBytes.length > curBlock.getDataBuffers()[colIndex].remaining()) {
            increaseBuffer(colIndex, stringBytes.length);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeNvarchar(curBlock.getDataBuffers()[colIndex], stringBytes);

        if (originalString == null) {
            markAsNull(colIndex);
        }
        columnsSet.set(colIndex);
    }

    public void setDate(int colIndex, Date date, ZoneId zone) {
        if (date == null) {
            markAsNull(colIndex);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeDate(curBlock.getDataBuffers()[colIndex], date == null ? 0 : dateToInt(date, zone));
        columnsSet.set(colIndex);
    }

    public void setDatetime(int colIndex, Timestamp timestamp, ZoneId zone) {
        if (timestamp == null) {
            markAsNull(colIndex);
        }
        blockFullness += ByteWriterFactory
                .getWriter(metadata.getType(colIndex))
                .writeDateTime(curBlock.getDataBuffers()[colIndex], timestamp == null ? 0 : dtToLong(timestamp, zone));
        columnsSet.set(colIndex);
    }

    private void initIterator(BlockDto block) {
        rowIterator = new RowIterator(block.getCapacity());
        rowIterator.next();
    }

    private void markAsNull(int index) {
        curBlock.getNullBuffers()[index].put((byte) 1);
        blockFullness++;
    }

    private void increaseBuffer(int index, int puttingStringLength) throws ConnException {
        int oldSize = curBlock.getDataBuffers()[index].capacity();
        int newSize;
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

    private boolean memoryLimitReached() {
        return blockFullness >= memoryLimit;
    }
}
