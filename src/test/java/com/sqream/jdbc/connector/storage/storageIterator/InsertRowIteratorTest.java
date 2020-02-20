package com.sqream.jdbc.connector.storage.storageIterator;

import org.junit.Test;

import static org.junit.Assert.*;

public class InsertRowIteratorTest {

    @Test
    public void nextTest() {
        int rowAmount = 10;
        RowIterator itr = new InsertRowIterator(rowAmount);

        for (int i = 0; i < rowAmount - 1; i++) {
            assertTrue(itr.next());
        }
        assertFalse(itr.next());
    }

    @Test
    public void resetTest() {
        int rowAmount = 10;
        RowIterator itr = new InsertRowIterator(rowAmount);
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
        RowIterator itr = new InsertRowIterator(rowAmount);

        for (int i = 0; i < rowAmount - 1; i++) {
            assertEquals(i, itr.getRowIndex());
            assertTrue(itr.next());
        }
        assertFalse(itr.next());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void whenRowPointerMoreMaxIndexThenThrowExceptionTest() {
        int rowAmount = 10;
        RowIterator itr = new InsertRowIterator(rowAmount);

        for (int i = 0; i < rowAmount - 1; i++) {
            assertEquals(i, itr.getRowIndex());
            assertTrue(itr.next());
        }
        assertFalse(itr.next());

        itr.getRowIndex();
    }
}