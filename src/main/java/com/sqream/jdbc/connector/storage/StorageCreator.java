package com.sqream.jdbc.connector.storage;

import java.nio.ByteBuffer;

public interface StorageCreator {
    StorageCreator fromFetchBuffers(ByteBuffer[] fetchBuffers);
    ColumnStorage build();
}
