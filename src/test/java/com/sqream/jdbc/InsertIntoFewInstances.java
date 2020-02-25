package com.sqream.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.sqream.jdbc.TestEnvironment.*;
import static junit.framework.TestCase.*;

public class InsertIntoFewInstances {


    private static final String SQL_CREATE_TABLE = "create or replace table public.perf (   " +
            "bools bool null,   bytes tinyint null,   shorts smallint null,   ints int null,   bigints bigint null,   " +
            "floats real null,   doubles double null,   strings varchar(10) null,   dates date null,   dts datetime null )  ;";
    private static final String SQL_INSERT = "insert into perf values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_SELECT_COUNT = "select count(*) from perf;";
    private static final int AMOUNT = 10_000_000;
    private static final int[] PORTS = new int[]{5000, 5001, 5002};

    private static final Logger log = Logger.getLogger(JDBCPerf.class.toString());

    private static Date date_from_tuple(int year, int month, int day) {
        return Date.valueOf(LocalDate.of(year, month, day));
    }

    private static Timestamp datetime_from_tuple(int year, int month, int day, int hour, int minutes, int seconds, int ms) {
        return Timestamp.valueOf(LocalDateTime.of(LocalDate.of(year, month, day), LocalTime.of(hour, minutes, seconds, ms * (int) Math.pow(10, 6))));
    }

    @Before
    public void setup() throws SQLException {
        Connection conn = createConnection();
        Statement statement = conn.createStatement();
        statement.execute(SQL_CREATE_TABLE);
        statement.close();
        conn.close();
    }

    @Test
    public void useFewInstancesTest() throws InterruptedException, SQLException {
        long t0 = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(PORTS.length);
        for (int port : PORTS) {
            executorService.submit(new InsertData(port));
        }
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);
        long t1 = System.currentTimeMillis();
        System.out.println(MessageFormat.format("Total time: [{0}] ms", t1 - t0));

        checkResult();
    }

    private class InsertData implements Runnable {

        private int port;

        public InsertData(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            String url = URL;
            if (url.contains(":5000")) {
                url = url.replace(":5000", ":" + port);
            } else {
                fail();
            }

            long t0 = System.nanoTime();
            try (Connection conn = createConnection();
                 PreparedStatement ps = conn.prepareStatement(SQL_INSERT)) {
                for (int j = 0; j < AMOUNT; j++) {
                    ps.setBoolean(1, true);
                    ps.setByte(2, (byte) 120);
                    ps.setShort(3, (short) 1400);
                    ps.setInt(4, 140000);
                    ps.setLong(5, (long) 5);
                    ps.setFloat(6, (float) 56.0);
                    ps.setDouble(7, 57.0);
                    ps.setString(8, "bla");
                    ps.setDate(9, date_from_tuple(2019, 11, 26));
                    ps.setTimestamp(10, datetime_from_tuple(2019, 11, 26, 16, 45, 23, 45));
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            long t1 = System.nanoTime();
            log.info(MessageFormat.format("Insert for {0} took: {1} ms\n", AMOUNT, (t1 - t0) / 1_000_000));
        }
    }

    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASS);
    }

    private void checkResult() throws SQLException {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(SQL_SELECT_COUNT);

            assertTrue(rs.next());
            assertEquals(AMOUNT * PORTS.length, rs.getLong(1));
        }
    }
}



