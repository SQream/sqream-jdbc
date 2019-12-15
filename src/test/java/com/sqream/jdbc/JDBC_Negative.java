package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.hamcrest.core.Is.isA;

@RunWith(JUnit4.class)
public class JDBC_Negative {

    private static final String SQL_CREATE_TABLE = "create or replace table perf " +
            "(bools bool, bytes tinyint, shorts smallint, ints int, bigints bigint, floats real, doubles double, " +
            "strings varchar(10), strangs nvarchar(10))"; //, dates date, dts datetime)";
    private static final int AMOUNT_OF_COLUMNS = 10;
    private static final String SQL_INSERT = "insert into perf values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_INSERT_LESS_MARKS_THAN_COLUMNS = "insert into perf values (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_SELECT_COUNT = "select count(*) from perf;";
    private static final String URI = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream";
    private static final String USER = "sqream";
    private static final String PASSWORD = "sqream";

    @Before
    public void setUp() throws SQLException {
        Connection conn = createConenction();
        Statement statement = conn.createStatement();
        statement.execute(SQL_CREATE_TABLE);
        statement.close();
        conn.close();
    }

    /**
     * Test case:
     *          In PreparedStatement passed template with 5 question marks.
     *          Try to set too much values (more than question marks in query template)
     *
     *          Expected SQLException instead of unchecked exception like NPE or ArrayIndexOutOfBoundsException.
     */
    @Test(expected = SQLException.class)
    public void whenSetWrongIndexInResultSet() throws SQLException {
        try (Connection conn = createConenction();
             PreparedStatement ps = conn.prepareStatement(SQL_INSERT)) {
            ps.setBoolean(1, true);
            ps.setByte(2, (byte)120);
            ps.setShort(3, (short) 1400);
            ps.setInt(4, 140000);
            ps.setLong(5, (long) 5);
            ps.setFloat(6, (float)56.0);
            ps.setDouble(7, 57.0);
            ps.setString(8, "bla");
            ps.setString(9, "bla2");
            ps.setDate(10, date_from_tuple(2019, 11, 26));
            ps.addBatch();
            ps.executeBatch();
        }
    }

    @Test(expected = SQLException.class)
    public void whenSetLessQuestionMarksThanColumnsInTable() throws SQLException {
        try (Connection conn = createConenction();
             PreparedStatement ps = conn.prepareStatement(SQL_INSERT_LESS_MARKS_THAN_COLUMNS)) {
        }
    }

    private Connection createConenction() throws SQLException {
        return DriverManager.getConnection(URI, USER, PASSWORD);
    }

    private static Date date_from_tuple(int year, int month, int day) {
        return Date.valueOf(LocalDate.of(year, month, day));
    }

    private static Timestamp datetime_from_tuple(int year, int month, int day, int hour, int minutes, int seconds, int ms) {
        return Timestamp.valueOf(LocalDateTime.of(LocalDate.of(year, month, day), LocalTime.of(hour, minutes, seconds, ms*(int)Math.pow(10, 6))));
    }
}
