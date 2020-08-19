package com.sqream.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

public class SQPreparedStatementTest {

    @Test
    public void setMaxRowsTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table test_max_rows (col1 int);";
        String INSERT_SQL_TEMPLATE = "insert into test_max_rows values (%s);";
        String SELECT_ALL_SQL = "select * from test_max_rows;";
        int maxRows = 3;
        int totalRows = 10;
        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_SQL);
                for (int i = 0; i < totalRows; i++) {
                    stmt.executeUpdate(String.format(INSERT_SQL_TEMPLATE, i));
                }
            }

            try (PreparedStatement pstmt = conn.prepareStatement(SELECT_ALL_SQL)) {
                pstmt.setMaxRows(maxRows);

                assertEquals(maxRows, pstmt.getMaxRows());
                ResultSet rs = pstmt.executeQuery();
                for (int i = 0; i < maxRows; i++) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getInt(1));
                }
                assertFalse(rs.next());
            }

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

    @Test
    public void setValueAsObjectTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table test_set_values_as_object " +
                "(col1 bool, col2 tinyint, col3 smallint, col4 int, " +
                "col5 bigint, col6 real, col7 double, " +
                "col11 date, col12 datetime);";
        String INSERT_SQL = "insert into test_set_values_as_object values (?, ?, ?, ?, ?, ?, ?, ?, ?);";
        String SELECT_ALL_SQL = "select * from test_set_values_as_object";
        List<Object> testValues = new ArrayList<>();

        Boolean booleanValue = true;
        byte byteValue = (byte) 5;
        short shortValue = (short) 10;
        int intValue = 42;
        long longValue = 42L;
        float floatValue = 42f;
        double doubleValue = 42d;
        String stringValue = "42";
        Date dateValue = new Date(System.currentTimeMillis());
        Timestamp timestampValue = new Timestamp(System.currentTimeMillis());

        testValues.add(booleanValue);
        testValues.add(byteValue);
        testValues.add(shortValue);
        testValues.add(intValue);
        testValues.add(longValue);
        testValues.add(floatValue);
        testValues.add(doubleValue);
        testValues.add(dateValue);
        testValues.add(timestampValue);

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_SQL);
            }

            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < testValues.size(); i++) {
                    ps.setObject(i + 1, testValues.get(i));
                }
                ps.addBatch();
                ps.executeBatch();
            }

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(SELECT_ALL_SQL);
                while (rs.next()) {
                    assertEquals(booleanValue, rs.getBoolean(1));
                    assertEquals(byteValue, rs.getByte(2), 0);
                    assertEquals(shortValue, rs.getShort(3), 0);
                    assertEquals(intValue, rs.getInt(4), 0);
                    assertEquals(longValue, rs.getLong(5), 0);
                    assertEquals(floatValue, rs.getFloat(6), 0);
                    assertEquals(doubleValue, rs.getDouble(7), 0);
                    assertEquals(dateValue.toString(), rs.getDate(8).toString());
                    assertEquals(timestampValue, rs.getTimestamp(9));
                }
            }
        }
    }

    @Test
    public void setVarcharAsObjectTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table test_set_values_as_object " +
                "(col1 varchar(10), col2 nvarchar(10));";
        String INSERT_SQL = "insert into test_set_values_as_object values (?, ?);";
        String SELECT_ALL_SQL = "select * from test_set_values_as_object";
        List<Object> testValues = new ArrayList<>();

        String stringValue = "42";
        testValues.add(stringValue);
        testValues.add(stringValue);

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_SQL);
            }

            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < testValues.size(); i++) {
                    ps.setObject(i + 1, testValues.get(i));
                }
                ps.addBatch();
                ps.executeBatch();
            }

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(SELECT_ALL_SQL);
                while (rs.next()) {
                    assertEquals(stringValue, rs.getString(1));
                    assertEquals(stringValue, rs.getString(2));
                }
            }
        }
    }

    @Test(expected = SQLException.class)
    public void setNullAsObject() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table test_set_null_as_object (col1 int);";
        String INSERT_SQL = "insert into test_set_null_as_object values (?);";

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_SQL);
            }

            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                ps.setObject(1,null);
            }
        }
    }

    @Test(expected = SQLException.class)
    public void setUnsupportedTypeAsObject() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table test_set_null_as_object (col1 int);";
        String INSERT_SQL = "insert into test_set_null_as_object values (?);";
        class UnsupportedType{};

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(CREATE_TABLE_SQL);
            }

            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                ps.setObject(1,new UnsupportedType());
            }
        }
    }

    @Test
    public void getStringRepeatedlyTest() throws SQLException {
        String CREATE_TABLE_SQL =
                "create or replace table test_get_string_repeatedly (col1 varchar(10), col2 nvarchar(10));";
        String INSERT_SQL = "insert into test_get_string_repeatedly values ('test_var', 'test_nvar');";
        String SELECT_SQL = "select * from test_get_string_repeatedly;";

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE_SQL);
            stmt.executeUpdate(INSERT_SQL);
            ResultSet rs = stmt.executeQuery(SELECT_SQL);

            if (rs.next()) {
                for (int i = 0; i < 10; i++) {
                    assertEquals("test_var", rs.getString(1));
                    assertEquals("test_nvar", rs.getString(2));
                }
            } else {
                fail("Could not read data from table");
            }
        }
    }

    @Test
    public void URLParamFetchSizeProvidedToPreparedStatementTest() throws SQLException {
        int FETCH_SIZE = 42;
        String URL_WITH_FETCH_SIZE = MessageFormat.format("{0};fetchSize={1}", URL, FETCH_SIZE);
        try (Connection conn = DriverManager.getConnection(URL_WITH_FETCH_SIZE,USER,PASS);
             PreparedStatement pstmt = conn.prepareStatement("Select 1;")) {

            assertEquals(FETCH_SIZE, pstmt.getFetchSize());
        }
    }

    @Test
    public void whenQueryTimeoutWasNotSpecifiedThenGetQueryTimeoutReturnZeroTest() throws SQLException {
        String createTable = "create or replace table test_table (col1 int);";
        String insert = "insert into test_table values (?);";
        int unlimited = 0;

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery(createTable);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(insert)) {
                Assert.assertEquals(unlimited, pstmt.getQueryTimeout());
            }

        }
    }

    @Test
    public void whenQueryTimeoutWasSpecifiedThenGetQueryTimeoutReturnCurrentValueTest() throws SQLException {
        String createTable = "create or replace table test_table (col1 int);";
        String insert = "insert into test_table values (?);";
        int timeout = 10;

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery(createTable);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(insert)) {
                pstmt.setQueryTimeout(timeout);
                Assert.assertEquals(timeout, pstmt.getQueryTimeout());
            }

        }
    }
}