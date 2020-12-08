package com.sqream.jdbc.connector.serverAPI.Statement;

/**
 * Representation of prepared statement.
 * In this state SQL query already sent to the server.
 */
public interface SqreamPreparedStatement extends CloseableSqreamStatement {
    SqreamExecutedStatement execute();
}
