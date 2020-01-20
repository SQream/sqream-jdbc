package com.sqream.jdbc;

import org.junit.Test;

import java.sql.*;
import java.util.stream.IntStream;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.assertEquals;

public class SQPreparedStatmentTest {

    public static final String IP = "127.0.0.1";
    public static final int PORT = 5000;
    public static final String DATABASE = "master";
    public static boolean CLUSTER = false;
    public static boolean SSL = false;
    public static String USER = "sqream";
    public static String PASS = "sqream";
    public static String SERVICE = "sqream";

    @Test
    public void setMaxRowsTest() throws SQLException {
        int maxRows = 3;
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.setMaxRows(maxRows);
        }
    }

    @Test
    public void executeBatchTest() throws SQLException {
        String createSql = "create or replace table test_exec (x int)";
        String insertSql = "insert into test_exec values (?)";
        String selectSql = "select * from test_exec";
        int randomInt = 8;
        int times = 10;
        int[] affectedRows;
        int insertedRows = 0;

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createSql);
        }
        try (Connection conn = createConnection();
             PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (int i = 0; i < times; i++) {
                ps.setInt(1, randomInt);
                ps.addBatch();
            }
            affectedRows = ps.executeBatch();
        }
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectSql)) {

            while (rs.next()) {
                insertedRows++;
            }
        }

        assertEquals(affectedRows.length, times);
        assertEquals(IntStream.of(affectedRows).sum(), times);
        assertEquals(insertedRows, times);
    }

    private Connection createConnection() {
        try {
            return DriverManager.getConnection(URL,USER,PASS);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}