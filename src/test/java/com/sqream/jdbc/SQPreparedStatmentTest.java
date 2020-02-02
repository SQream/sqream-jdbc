package com.sqream.jdbc;

import org.junit.Test;

import java.sql.*;
import java.util.stream.IntStream;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

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

    @Test(expected = SQLException.class)
    public void executeWithParamThrowException1Test() throws SQLException {
        String sql = "select 1";

        try (Connection conn = createConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.execute(sql);
        }
    }

    @Test(expected = SQLException.class)
    public void executeWithParamThrowException2Test() throws SQLException {
        String sql = "select 1";

        try (Connection conn = createConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.execute(sql, new int[0]);
        }
    }

    @Test(expected = SQLException.class)
    public void executeWithParamThrowException3Test() throws SQLException {
        String sql = "select 1";

        try (Connection conn = createConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.execute(sql, 1);
        }
    }

    @Test(expected = SQLException.class)
    public void executeWithParamThrowException4Test() throws SQLException {
        String sql = "select 1";

        try (Connection conn = createConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.execute(sql, new String[0]);
        }
    }

    @Test
    public void executeReturnResultDependsOnStatementTypeTest() throws SQLException {
        String createTable = "create or replace table check_select_statement (col1 int)";
        String insertData = "insert into check_select_statement values (42)";
        String selectData = "select * from check_select_statement";
        String dropTable = "drop table check_select_statement";
        boolean createResult;
        boolean insertResult;
        boolean selectResult;
        boolean deleteResult;

        try (Connection conn = createConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(createTable)) {
                createResult = ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(insertData)) {
                insertResult = ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(selectData)) {
                selectResult = ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(dropTable)) {
                deleteResult = ps.execute();
            }
        }

        assertFalse(createResult);
        assertFalse(insertResult);
        assertTrue(selectResult);
        assertFalse(deleteResult);
    }

    private Connection createConnection() {
        try {
            return DriverManager.getConnection(URL,USER,PASS);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}