package com.sqream.jdbc.connector.serverAPI.Statement;

import com.sqream.jdbc.connector.ConnException;

/**
 * Representation of created statement.
 * Does not have close() method because of the specifics in protocol implementation.
 * In this state only {@link #prepare(String)} can be called.
 */
public interface SqreamCreatedStatement extends SqreamStatement {
    SqreamPreparedStatement prepare(String query) throws ConnException;
}
