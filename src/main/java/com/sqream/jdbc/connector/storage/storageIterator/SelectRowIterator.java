package com.sqream.jdbc.connector.storage.storageIterator;

import java.text.MessageFormat;

public class SelectRowIterator implements RowIterator {
    private static final int INIT_ROW_INDEX = -1;
    private static final int RESET_ROW_INDEX = 0;

    private int rowIndex = INIT_ROW_INDEX;
    private int rowAmount;
    private boolean firstIteration = true;

    public SelectRowIterator(int rowAmount) {
        this.rowAmount = rowAmount;
    }

    /**
     * Initially the cursor is positioned before the first row. \
     * After call reset method the cursor is positioned on the first row.
     */
    @Override
    public boolean next() {
        if (firstIteration) {
            return ++rowIndex < rowAmount;
        } else {
            return rowIndex < rowAmount;
        }
    }

    @Override
    public void reset() {
        rowIndex = RESET_ROW_INDEX;
    }

    @Override
    public int getRowIndex() {
        if (rowIndex < 0 || rowIndex >= rowAmount) {
            throw new IndexOutOfBoundsException(MessageFormat.format(
                    "Call next() before getRowIndex() or reset iterator. Current rowIndex=[{0}], rowAmount=[{1}]",
                    rowIndex, rowAmount));
        }
        return rowIndex;
    }
}
