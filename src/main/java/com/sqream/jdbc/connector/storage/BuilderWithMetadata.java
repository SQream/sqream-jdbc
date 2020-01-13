package com.sqream.jdbc.connector.storage;

import com.sqream.jdbc.connector.TableMetadata;

public interface BuilderWithMetadata {
    BuilderWithBlockSize withMetadata(TableMetadata metadata);
}
