package com.sqream.jdbc.connector.serverAPI.Statement;

/**
 * Base interface for statements. Every specific statement should extend this interface.
 */
public interface SqreamStatement {
    int getId();
    boolean isOpen();
}
