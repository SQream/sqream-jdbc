package com.sqream.jdbc.connector.storage.storageIterator;

public interface RowIterator {

    boolean next();

    void reset();

    int getRowIndex();
}
