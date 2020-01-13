package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.TableMetadata;

import java.nio.ByteBuffer;

public class ColumnStorageBuilder implements BuilderWithMetadata, BuilderWithBlockSize, StorageCreator {

    private TableMetadata metadata;
    private int blockSize;
    private ByteBuffer[] fetchBuffers;

    ColumnStorageBuilder() { }

    public BuilderWithBlockSize withMetadata(TableMetadata metadata) {
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

