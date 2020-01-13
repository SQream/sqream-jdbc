package com.sqream.jdbc.connector;

import java.nio.ByteBuffer;

public class ColumnStorageBuilder implements WithMetadata, WithBlockSize, StorageCreator {

    private TableMetadata metadata;
    private int blockSize;
    private ByteBuffer[] fetchBuffers;

    ColumnStorageBuilder() { }

    public WithBlockSize withMetadata(TableMetadata metadata) {
        this.metadata = metadata;
        return this;
    }

    public StorageCreator withBlockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    @Override
    public StorageCreator fromFetchBuffers(ByteBuffer[] fetchBuffers) {
        this.fetchBuffers = fetchBuffers;
        return this;
    }

    public ColumnStorage build() {
        ColumnStorage result = new ColumnStorage();
        if (fetchBuffers == null) {
            result.init(metadata, blockSize);
        } else {
            result.initFromFetch(fetchBuffers, metadata, blockSize);
        }
        return result;
    }

}

interface WithMetadata {
    WithBlockSize withMetadata(TableMetadata metadata);
}

interface WithBlockSize {
    StorageCreator withBlockSize(int blockSize);
}

interface StorageCreator {
    StorageCreator fromFetchBuffers(ByteBuffer[] fetchBuffers);
    ColumnStorage build();
}

