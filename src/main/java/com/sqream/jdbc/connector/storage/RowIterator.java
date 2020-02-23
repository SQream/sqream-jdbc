package com.sqream.jdbc.connector.storage;

import java.text.MessageFormat;
import java.util.NoSuchElementException;

public class RowIterator {

    private int rowIndex = -1;
    private int rowAmount;

    public RowIterator(int rowAmount) {
        this.rowAmount = rowAmount;
    }

    /**
     * Initially the cursor is positioned before the first row.
     */
    public boolean next() {
        return ++rowIndex < rowAmount;
    }

    public int getRowIndex() {
        return rowIndex;
    }
}
