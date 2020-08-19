package com.sqream.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static com.sqream.jdbc.TestEnvironment.createConnection;

public class SQStatementNotSupportedTest {
    private static Statement stmt;

    @BeforeClass
    public static void setUp() throws SQLException {
        try (Connection conn = createConnection();
             Statement statement = conn.createStatement()) {

            stmt = statement;
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void clearBatch() throws SQLException {
        stmt.clearBatch();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void clearWarnings() throws SQLException {
        stmt.clearWarnings();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdate() throws SQLException {
        stmt.executeUpdate(null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdate2() throws SQLException {
        stmt.executeUpdate(null, new int[0]);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdate3() throws SQLException {
        stmt.executeUpdate(null, new String[0]);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void execute() throws SQLException {
        stmt.execute(null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void execute2() throws SQLException {
        stmt.execute(null, new int[0]);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void execute3() throws SQLException {
        stmt.execute(null, new String[0]);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeBatch() throws SQLException {
        stmt.executeBatch();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void unwrap() throws SQLException {
        stmt.unwrap(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void addBatch() throws SQLException {
        stmt.addBatch(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getFetchDirection() throws SQLException {
        stmt.getFetchDirection();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getGeneratedKeys() throws SQLException {
        stmt.getGeneratedKeys();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getMaxFieldSize() throws SQLException {
        stmt.getMaxFieldSize();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getResultSetConcurrency() throws SQLException {
        stmt.getResultSetConcurrency();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getResultSetHoldability() throws SQLException {
        stmt.getResultSetHoldability();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getResultSetType() throws SQLException {
        stmt.getResultSetType();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void isPoolable() throws SQLException {
        stmt.isPoolable();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCursorName() throws SQLException {
        stmt.setCursorName("");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setFetchDirection() throws SQLException {
        stmt.setFetchDirection(0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setMaxFieldSize() throws SQLException {
        stmt.setMaxFieldSize(0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setPoolable() throws SQLException {
        stmt.setPoolable(false);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void closeOnCompletion() throws SQLException {
        stmt.closeOnCompletion();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void isCloseOnCompletion() throws SQLException {
        stmt.isCloseOnCompletion();
    }

}