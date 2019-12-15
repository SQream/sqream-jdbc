package com.sqream.jdbc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.logging.Logger;

public class JDBCPerf {

    private static final String SQL_CREATE_TABLE = "create or replace table perf " +
            "(bools bool, bytes tinyint, shorts smallint, ints int, bigints bigint, floats real, doubles double, " +
            "strings varchar(10), strangs nvarchar(10), dates date, dts datetime)";
    private static final String SQL_INSERT = "insert into perf values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_SELECT_COUNT = "select count(*) from perf;";
    private static final String URI = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream";
    private static final String USER = "sqream";
    private static final String PASSWORD = "sqream";
    private static final int AMOUNT = 10_000_000;

    private static final Logger log = Logger.getLogger(JDBCPerf.class.toString());

    private static Date date_from_tuple(int year, int month, int day) {
        return Date.valueOf(LocalDate.of(year, month, day));
    }

    private static Timestamp datetime_from_tuple(int year, int month, int day, int hour, int minutes, int seconds, int ms) {
        return Timestamp.valueOf(LocalDateTime.of(LocalDate.of(year, month, day), LocalTime.of(hour, minutes, seconds, ms*(int)Math.pow(10, 6))));
    }

    public static void main(String[] args) throws SQLException {
        new JDBCPerf().test();
    }

    @Before
    public void setup() throws SQLException {
        Connection conn = createConenction();
        Statement statement = conn.createStatement();
        statement.execute(SQL_CREATE_TABLE);
        statement.close();
        conn.close();
    }

    private Connection createConenction() throws SQLException {
        return DriverManager.getConnection(URI, USER, PASSWORD);
    }

    @Test
    public void test() throws SQLException {

        Connection conn = createConenction();
        PreparedStatement ps = conn.prepareStatement(SQL_INSERT);

        long t0 = System.nanoTime();
        for (int j = 0; j < AMOUNT; j++) {
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
            ps.setTimestamp(11, datetime_from_tuple(2019, 11, 26, 16, 45, 23, 45));
            ps.addBatch();
        }
        long t1 = System.nanoTime();

        log.info(MessageFormat.format("Insert for {0} took: {1} ms\n", AMOUNT, (t1-t0)/1_000_000));
        ps.close();

        checkResult(conn);
        conn.close();
    }

    private void checkResult(Connection conn) throws SQLException {
        Statement statement = conn.createStatement();
        statement.execute(SQL_SELECT_COUNT);
        ResultSet rs = statement.getResultSet();
        String expected = null;
        if (rs.next()) {
            expected = rs.getString(1);
        }
        Assert.assertNotNull(expected);
        Assert.assertTrue(Integer.valueOf(expected).equals(AMOUNT));
    }
}