package com.sqream.jdbc.connector.storage;

public interface BuilderWithBlockSize {
    StorageCreator withBlockSize(int blockSize);
}
