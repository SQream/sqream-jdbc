package com.sqream.jdbc;

import org.junit.Test;

import java.sql.*;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    private Connection createConnection() {
        try {
            Class.forName("com.sqream.jdbc.SQDriver");
            return DriverManager.getConnection(URL,USER,PASS);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
