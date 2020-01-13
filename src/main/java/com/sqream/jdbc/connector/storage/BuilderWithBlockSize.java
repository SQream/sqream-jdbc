package com.sqream.jdbc.connector.storage;

public interface BuilderWithBlockSize {
    StorageCreator blockSize(int blockSize);
}
