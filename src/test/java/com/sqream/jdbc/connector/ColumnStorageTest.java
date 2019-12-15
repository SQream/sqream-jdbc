package com.sqream.jdbc.connector;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class ColumnStorageTest {

    private ColumnStorage storage;
    private static final int ROW_LENGTH = 10;
    private static final int ROWS_PER_FLUSH = 5;

    @Before
    public void setUp() {
        storage = new ColumnStorage();
        storage.init(ROW_LENGTH);
        storage.setNullReseter(ROW_LENGTH);
    }

    @Test
    public void reloadTest() {
        int newRowLength = ROW_LENGTH * 2;
        ByteBuffer[] dataBuffer = new ByteBuffer[newRowLength];
        ByteBuffer[] nullBuffer = new ByteBuffer[newRowLength];
        ByteBuffer[] nvarcLenBuffer = new ByteBuffer[newRowLength];

        storage.reload(dataBuffer, nullBuffer, nvarcLenBuffer);

        assertEquals(newRowLength, storage.getDataColumns().length);
        assertEquals(newRowLength, storage.getNullColumns().length);
        assertEquals(newRowLength, storage.getNvarcLenColumns().length);
    }

    @Test
    public void initDataColumnsTest() {
        for (int i = 0; i < ROW_LENGTH; i++) {
            int initSize = i * 2;
            assertNull(storage.getDataColumns(i));
            storage.initDataColumns(i, initSize);
            assertEquals(initSize, storage.getDataColumns(i).capacity());
        }
    }

    @Test
    public void initNullColumnsTest() {
        for (int i = 0; i < ROW_LENGTH; i++) {
            int initSize = i * 2;
            assertNull(storage.getNullColumn(i));
            storage.initNullColumns(i, initSize);
            assertEquals(initSize, storage.getNullColumn(i).capacity());
        }
    }

    @Test
    public void initNvarcLenColumnsTest() {
        for (int i = 0; i < ROW_LENGTH; i++) {
            int initSize = i * 2;
            int expectedSize = initSize * 4;
            assertNull(storage.getNvarcLenColumn(i));
            storage.initNvarcLenColumns(i, initSize);
            assertEquals(expectedSize, storage.getNvarcLenColumn(i).capacity());
        }
    }

    @Test
    public void resetNullColumnsTest() {
        for (int i = 0; i < ROW_LENGTH; i++) {
            int testValue = 42;
            storage.setNullColumns(i, ByteBuffer.allocate(1));
            assertNotNull(storage.getNullColumn(i));
            storage.resetNullColumns(i);
            assertNull(storage.getNullColumn(i));
        }
    }

    @Test
    public void resetNvarcLenColumnsTest() {
        for (int i = 0; i < ROW_LENGTH; i++) {
            int testValue = 42;
            storage.setNvarcLenColumns(i, ByteBuffer.allocate(1));
            assertNotNull(storage.getNvarcLenColumn(i));
            storage.resetNvarcLenColumns(i);
            assertNull(storage.getNvarcLenColumn(i));
        }
    }

    @Test
    public void setGetDataColumnsTest() {
        initColumns();
        ByteBuffer buffer = ByteBuffer.allocate(ROW_LENGTH);
        for (int i = 0; i < ROW_LENGTH; i++) {
            buffer.putInt(i);
            buffer.flip();
            storage.setDataColumns(i, buffer);
            assertEquals(i, storage.getDataColumns(i).getInt());
            buffer.flip();
        }
    }

    @Test
    public void setGetNullColumnsTest() {
        initColumns();
        ByteBuffer buffer = ByteBuffer.allocate(ROW_LENGTH);
        for (int i = 0; i < ROW_LENGTH; i++) {
            buffer.putInt(i);
            buffer.flip();
            storage.setNullColumns(i, buffer);
            assertEquals(i, storage.getNullColumn(i).getInt());
            buffer.flip();
        }
    }

    @Test
    public void setGetNvarcLenColumnsTest() {
        initColumns();
        ByteBuffer buffer = ByteBuffer.allocate(ROW_LENGTH);
        for (int i = 0; i < ROW_LENGTH; i++) {
            buffer.putInt(i);
            buffer.flip();
            storage.setNvarcLenColumns(i, buffer);
            assertEquals(i, storage.getNvarcLenColumn(i).getInt());
            buffer.flip();
        }
    }

    private void initColumns() {
        for (int i = 0; i < ROW_LENGTH; i++) {
            int size = i * 2;
            storage.initDataColumns(i, size);
            storage.initNullColumns(i, size);
            storage.initNvarcLenColumns(i, size);
        }
    }
}