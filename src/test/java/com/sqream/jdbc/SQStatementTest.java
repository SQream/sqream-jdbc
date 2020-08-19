package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.text.MessageFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

public class SQStatementTest {
    private static final String testTableForDelay = "delay_test";

    @BeforeClass
    public static void setUp() throws SQLException {
        int AMOUNT = 28;
        String createTable = MessageFormat.format("create or replace table {0} (col1 int);", testTableForDelay);
        String insertRow = MessageFormat.format("insert into {0} values (1);", testTableForDelay);
        String multiply = MessageFormat.format("insert into {0} select * from {0};", testTableForDelay);

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate(createTable);
            stmt.executeUpdate(insertRow);
            for (int i = 0; i < AMOUNT; i++) {
                stmt.executeUpdate(multiply);
            }
        }
    }

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

    @Test(expected = SQLException.class)
    public void correctExceptionMessageTest() throws SQLException {
        String sql = "CREATE TABLE nba_hdfs\n" +
                "(\n" +
                "     \"Name\"       TEXT,\n" +
                "     \"Team\"       TEXT,\n" +
                "     \"Number\"     BIGINT,\n" +
                "     \"Position\"   VARCHAR(2),\n" +
                "     \"Age\"        BIGINT,\n" +
                "     \"Height\"     VARCHAR(4),\n" +
                "     \"Weight\"     BIGINT,\n" +
                "     \"College\"    TEXT,\n" +
                "     \"Salary\"     FLOAT\n" +
                " );\n" +
                "\n" +
                "COPY nba_hdfs FROM 'hdfs://192.168.6.51/arnon/*.csv' \n" +
                "   WITH \n" +
                "     OFFSET 2\n" +
                "     RECORD DELIMITER '\\r\\n';";
        String prefix = "can not execute - ";
        String expectedStartMessage = prefix + "ParseSql.parseStatement: expected one statement";

        try (Connection conn = createConnection();
             Statement stmt=conn.createStatement()) {

            stmt.execute(sql);
        } catch (SQLException e) {
            String actualStartMessage = e.getMessage().substring(0, expectedStartMessage.length());
            if (expectedStartMessage.equals(actualStartMessage)) {
                throw e;
            } else {
                fail(MessageFormat.format("Unexpected start of message. Expected: [{0}], Actual: [{1}]",
                        expectedStartMessage, actualStartMessage));
            }
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
            try (Statement stmt = conn.createStatement()) {
                createResult = stmt.execute(createTable);
            }
            try (Statement stmt = conn.createStatement()) {
                insertResult = stmt.execute(insertData);
            }
            try (Statement stmt = conn.createStatement()) {
                selectResult = stmt.execute(selectData);
            }
            try (Statement stmt = conn.createStatement()) {
                deleteResult = stmt.execute(dropTable);
            }
        }

        assertFalse(createResult);
        assertFalse(insertResult);
        assertTrue(selectResult);
        assertFalse(deleteResult);
    }

    @Test
    public void executeUpdateTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table execute_statement_test (col1 int);";
        String INSERT_SQL = "insert into execute_statement_test values (42);";
        String SELECT_SQL = "select * from execute_statement_test;";

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE_SQL);

            stmt.executeUpdate(INSERT_SQL);

            ResultSet rs = stmt.executeQuery(SELECT_SQL);
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }

    }

    @Test
    public void executeTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table execute_statement_test (col1 int);";
        String INSERT_SQL = "insert into execute_statement_test values (42);";
        String SELECT_SQL = "select * from execute_statement_test;";

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE_SQL);

            stmt.execute(INSERT_SQL);

            ResultSet rs = stmt.executeQuery(SELECT_SQL);
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test(expected = SQLException.class)
    public void cancelExecutableStatementTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table cancel_statement_test (col1 int);";

        StringBuilder INSERT_SQL = new StringBuilder("insert into cancel_statement_test values (42)");
        for (int i = 0; i < 1_000; i++) {
            INSERT_SQL.append(", (42)");
        }
        INSERT_SQL.append(";");

        String SELECT_SQL = "select count(*) from cancel_statement_test;";

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE_SQL);

            ExecutorService cancelExecutor = Executors.newSingleThreadExecutor();
            cancelExecutor.submit(() -> {
                try {
                    Thread.sleep(1000);
                    stmt.cancel();
                } catch (InterruptedException | SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            stmt.executeUpdate(INSERT_SQL.toString());
        }

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(SELECT_SQL);

            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
        }
    }

    @Test
    public void cancelStatementAfterExecuteTest() throws SQLException {
        String CREATE_TABLE_SQL = "create or replace table cancel_statement_test (col1 int);";
        String INSERT_SQL = "insert into cancel_statement_test values (42);";
        String SELECT_SQL = "select count(*) from cancel_statement_test;";

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE_SQL);

            stmt.executeUpdate(INSERT_SQL);

            stmt.cancel();
        }

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(SELECT_SQL);

            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
        }
    }

    @Test
    public void closeClosedStatementTest() throws SQLException {
        Statement stmtCopy;
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmtCopy = stmt;
        }

        stmtCopy.close();
    }

    @Test
    public void setFetchSizeTest() throws SQLException {
        int FETCH_SIZE = 1;

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.setFetchSize(FETCH_SIZE);
            ResultSet rs = stmt.executeQuery("select 1;");

            assertEquals(FETCH_SIZE, stmt.getFetchSize());
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void whenFetchSizeLessThanFetchedBlockProcessingOneByOneTest() throws SQLException {
        int FETCH_SIZE = 1;
        int AMOUNT_OF_ROWS = ConnectorImpl.ROWS_PER_FLUSH * 3;
        String CREATE_TABLE_SQL = "create or replace table fetch_size_test(col1 int);";
        String INSERT_SQL = "insert into fetch_size_test values (?);";
        String SELECT_SQL = "select * from fetch_size_test;";

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(CREATE_TABLE_SQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < AMOUNT_OF_ROWS; i++) {
                    pstmt.setInt(1, i);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.setFetchSize(FETCH_SIZE);
                ResultSet rs = stmt.executeQuery(SELECT_SQL);
                for (int i = 0; i < AMOUNT_OF_ROWS; i++) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getInt(1));
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void whenFetchSizeMoreThanFetchedBlockProcessingFewBlocksAtOneFetchTest() throws SQLException {
        int FETCH_SIZE = ConnectorImpl.ROWS_PER_FLUSH * 3 + 1;
        int AMOUNT_OF_ROWS = ConnectorImpl.ROWS_PER_FLUSH * 10;
        String CREATE_TABLE_SQL = "create or replace table fetch_size_test(col1 int);";
        String INSERT_SQL = "insert into fetch_size_test values (?);";
        String SELECT_SQL = "select * from fetch_size_test;";

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(CREATE_TABLE_SQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < AMOUNT_OF_ROWS; i++) {
                    pstmt.setInt(1, i);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.setFetchSize(FETCH_SIZE);
                ResultSet rs = stmt.executeQuery(SELECT_SQL);
                for (int i = 0; i < AMOUNT_OF_ROWS; i++) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getInt(1));
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void whenFetchSizeMoreThanAmountOfRowsInTableTest() throws SQLException {
        int FETCH_SIZE = 100;
        int AMOUNT_OF_ROWS = 10;
        String CREATE_TABLE_SQL = "create or replace table fetch_size_test(col1 int);";
        String INSERT_SQL = "insert into fetch_size_test values (?);";
        String SELECT_SQL = "select * from fetch_size_test;";

        try (Connection conn = createConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(CREATE_TABLE_SQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < AMOUNT_OF_ROWS; i++) {
                    pstmt.setInt(1, i);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.setFetchSize(FETCH_SIZE);
                ResultSet rs = stmt.executeQuery(SELECT_SQL);
                for (int i = 0; i < AMOUNT_OF_ROWS; i++) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getInt(1));
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void URLParamFetchSizeProvidedToStatementTest() throws SQLException {
        int FETCH_SIZE = 42;
        String URL_WITH_FETCH_SIZE = MessageFormat.format("{0};fetchSize={1}", URL, FETCH_SIZE);
        try (Connection conn = DriverManager.getConnection(URL_WITH_FETCH_SIZE,USER,PASS);
             Statement stmt = conn.createStatement()) {

            assertEquals(FETCH_SIZE, stmt.getFetchSize());
        }
    }

    @Test
    public void whenQueryTimeoutWasNotSpecifiedThenGetQueryTimeoutReturnZeroTest() throws SQLException {
        int unlimited = 0;

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            Assert.assertEquals(unlimited, stmt.getQueryTimeout());
        }
    }

    @Test
    public void whenQueryTimeoutWasSpecifiedThenGetQueryTimeoutReturnCurrentValueTest() throws SQLException {
        int timeout = 10;

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.setQueryTimeout(timeout);
            Assert.assertEquals(timeout, stmt.getQueryTimeout());
        }
    }

    @Test(expected = SQLTimeoutException.class)
    public void whenExecuteQueryReachTimeoutThrowExceptionTest() throws SQLException {
        int timeout = 1;
        String heavyStatement =
                MessageFormat.format("insert into {0} select * from {0};", testTableForDelay, testTableForDelay);


        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(timeout);
                stmt.executeQuery(heavyStatement);
            } catch (SQLTimeoutException e) {
                assertTrue(serverQueueEmpty());
                throw e;
            }
        }
        Assert.fail("Should catch SQLTimeoutException");
    }

    @Test(expected = SQLTimeoutException.class)
    public void whenExecuteReachTimeoutThrowExceptionTest() throws SQLException {
        int timeout = 1;
        String heavyStatement =
                MessageFormat.format("insert into {0} select * from {0};", testTableForDelay, testTableForDelay);


        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(timeout);
                stmt.execute(heavyStatement);
            } catch (SQLTimeoutException e) {
                assertTrue(serverQueueEmpty());
                throw e;
            }
        }
        Assert.fail("Should catch SQLTimeoutException");
    }

    @Test(expected = SQLTimeoutException.class)
    public void whenExecuteUpdateReachTimeoutThrowExceptionTest() throws SQLException {
        int timeout = 1;
        String heavyStatement =
                MessageFormat.format("insert into {0} select * from {0};", testTableForDelay, testTableForDelay);


        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(timeout);
                stmt.executeUpdate(heavyStatement);
            } catch (SQLTimeoutException e) {
                assertTrue(serverQueueEmpty());
                throw e;
            }
        }
        Assert.fail("Should catch SQLTimeoutException");
    }

    public boolean serverQueueEmpty() throws SQLException {
        String select = "select show_server_status()";

        try(Connection conn = createConnection();
            Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(select);
            return rs.next() && ("select show_server_status()".equals(rs.getString(10)) && !rs.next());
        }
    }
}
