package com.sqream.jdbc.connector.storage.storageIterator;

import com.sqream.jdbc.connector.storage.RowIterator;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class RowIteratorTest {

    @Test
    public void nextTest() {
        int rowAmount = 10;
        RowIterator itr = new RowIterator(rowAmount);

        for (int i = 0; i < rowAmount; i++) {
            assertTrue(itr.next());
        }
        assertFalse(itr.next());
    }

    @Test
    public void getRowIndexTest() {
        int rowAmount = 10;
        RowIterator itr = new RowIterator(rowAmount);

        for (int i = 0; i < rowAmount; i++) {
            assertTrue(itr.next());
            assertEquals(i, itr.getRowIndex());
        }
        assertFalse(itr.next());
    }

    @Test
    public void whenZeroRowsThenNextReturnFalseTest() {
        int rowAmount = 0;
        RowIterator itr = new RowIterator(rowAmount);

        assertFalse(itr.next());
    }
}