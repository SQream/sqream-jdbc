package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;
import org.junit.Test;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.text.MessageFormat;

import static com.sqream.jdbc.TestEnvironment.URL;
import static org.junit.Assert.*;

public class SQStatementTest {

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

    private Connection createConnection() {
        try {
            return DriverManager.getConnection(URL,"sqream","sqream");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}