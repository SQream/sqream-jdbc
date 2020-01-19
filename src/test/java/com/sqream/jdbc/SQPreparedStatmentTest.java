package com.sqream.jdbc;

import org.junit.Test;

import java.sql.*;

public class SQPreparedStatmentTest {
    private static final String url =
            "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false;service=sqream";

    @Test
    public void setMaxRowsTest() throws SQLException {
        int maxRows = 3;
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.setMaxRows(maxRows);
        }
    }

    private Connection createConnection() {
        try {
            return DriverManager.getConnection(url,"sqream","sqream");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}