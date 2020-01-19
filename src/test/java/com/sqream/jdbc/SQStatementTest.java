package com.sqream.jdbc;

import org.junit.Test;
import sun.awt.X11.XStateProtocol;

import java.sql.*;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class SQStatementTest {

    private static final String url =
            "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false;service=sqream";

    @Test
    public void setMaxRowsTest() throws SQLException {
        String createSql = "create or replace table test_fetch (ints int)";
        String insertSql = "insert into test_fetch values (1), (2), (3), (4), (5)";
        String selectSql = "select * from test_fetch";

        int maxRows = 3;
        int count = 0;

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createSql);
            stmt.execute(insertSql);
            stmt.setMaxRows(maxRows);
            ResultSet rs = stmt.executeQuery(selectSql);
            while(rs.next()) {
                rs.getInt(1);
                count++;
            }
        }

        assertEquals(count, maxRows);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeBatchTest() throws SQLException {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.executeBatch();
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