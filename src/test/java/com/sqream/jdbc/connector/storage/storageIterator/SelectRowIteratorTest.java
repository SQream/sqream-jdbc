package com.sqream.jdbc.connector.storage.storageIterator;

import org.junit.Test;

import static org.junit.Assert.*;

public class SelectRowIteratorTest {

    @Test
    public void nextTes() {
        int rowAmount = 10;
        RowIterator itr = new SelectRowIterator(rowAmount);

        for (int i = 0; i < rowAmount; i++) {
            assertTrue(itr.next());
        }
        assertFalse(itr.next());
    }

    @Test
    public void resetTest() {
        int rowAmount = 10;
        RowIterator itr = new SelectRowIterator(rowAmount);
        while (itr.next()) {}

        itr.reset();
        int rowAmountAfterReset = 1;
        while (itr.next()) {
            rowAmountAfterReset++;
        }

        assertEquals(rowAmount, rowAmountAfterReset);
    }

    @Test
    public void getRowIndexTest() {
        int rowAmount = 10;
        RowIterator itr = new SelectRowIterator(rowAmount);

        for (int i = 0; i < rowAmount; i++) {
            assertTrue(itr.next());
            assertEquals(i, itr.getRowIndex());
        }
        assertFalse(itr.next());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void whenRowPointerLessZeroThenThrowException() {
        int rowAmount = 10;
        RowIterator itr = new SelectRowIterator(rowAmount);
        itr.getRowIndex();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void whenRowPointerMoreMaxIndexThenThrowException() {
        int rowAmount = 10;
        RowIterator itr = new SelectRowIterator(rowAmount);

        for (int i = 0; i < rowAmount; i++) {
            assertTrue(itr.next());
            assertEquals(i, itr.getRowIndex());
        }
        assertFalse(itr.next());

        itr.getRowIndex();
    }
}