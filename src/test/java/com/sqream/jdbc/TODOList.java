package com.sqream.jdbc;

import org.junit.Test;

import java.sql.*;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

public class TODOList {

    //TODO: Should we close SQPreparedStatement#executeQuery() as "Not supported" or implement this logic?
    @Test
    public void executeQueryTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table execute_query_test(col1 int)";
        String INSERT_SQL = "insert into execute_query_test values(?)";
        String SELECT_SQL = "select * from execute_query_test";
        int TEST_VALUE = 42;

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_SQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {
                pstmt.setInt(1, TEST_VALUE);
                pstmt.executeQuery();
            }

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(SELECT_SQL);
                assertTrue(rs.next());
                assertEquals(TEST_VALUE, rs.getInt(1));
            }
        }
    }

    //TODO: If client set value on the same row few times, value should be overwritten.
    // Now every time when we write into storage, position in byteBuffer moves forward until BufferOverflowException
    @Test
    public void whenSetInPreparedStatementWithoutCallingAddBatchOrExecuteThenValuesAreNotOverwrittenTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table execute_update_test(col1 int)";
        String INSERT_SQL = "insert into execute_update_test values (?)";
        String SELECT_ALL_SQL = "select * from execute_update_test";
        int TEST_VALUE = 42;
        int AMOUNT = 1_000_000_000;

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_SQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < AMOUNT; i++) {
                    pstmt.setInt(1, TEST_VALUE);
                }
                pstmt.executeUpdate();
            }

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(SELECT_ALL_SQL);

                assertTrue(rs.next());
                assertEquals(TEST_VALUE, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    //TODO: Rewrite commented tests in SQSocketConnectorTest
}
