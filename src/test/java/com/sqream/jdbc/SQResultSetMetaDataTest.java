package com.sqream.jdbc;

import org.junit.Test;

import java.sql.*;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

public class SQResultSetMetaDataTest {

    @Test
    public void isCurrencyTest() throws SQLException {

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSetMetaData metaData = stmt.executeQuery("select 1;").getMetaData();
            assertFalse(metaData.isCurrency(1));
        }
    }

    @Test
    public void columnDisplaySizeTest() throws SQLException {
        String createSql = "create or replace table test_display_size " +
                "(col1 bool, col2 tinyint, col3 smallint, col4 int, col5 bigint, col6 real, col7 double, " +
                "col8 varchar(10), col9 nvarchar(10), col10 text(10), col11 nvarchar, col12 text, col13 date, " +
                "col14 datetime, col15 numeric(20,5));";
        String selectSql = "select * from test_display_size";

        ResultSetMetaData rsmeta;
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createSql);
            ResultSet rs = stmt.executeQuery(selectSql);
            rsmeta = rs.getMetaData();
            rs.close();
        }

        assertNotNull(rsmeta);

        // check getColumnDisplaySize
        assertEquals(1, rsmeta.getColumnDisplaySize(1));
        assertEquals(3, rsmeta.getColumnDisplaySize(2));
        assertEquals(6, rsmeta.getColumnDisplaySize(3));
        assertEquals(11, rsmeta.getColumnDisplaySize(4));
        assertEquals(20, rsmeta.getColumnDisplaySize(5));
        assertEquals(10, rsmeta.getColumnDisplaySize(6));
        assertEquals(12, rsmeta.getColumnDisplaySize(7));
        assertEquals(10, rsmeta.getColumnDisplaySize(8));
        assertEquals(Integer.MAX_VALUE, rsmeta.getColumnDisplaySize(9));
        assertEquals(Integer.MAX_VALUE, rsmeta.getColumnDisplaySize(10));
        assertEquals(Integer.MAX_VALUE, rsmeta.getColumnDisplaySize(11));
        assertEquals(Integer.MAX_VALUE, rsmeta.getColumnDisplaySize(12));
        assertEquals(10, rsmeta.getColumnDisplaySize(13));
        assertEquals(23, rsmeta.getColumnDisplaySize(14));
        assertEquals(40, rsmeta.getColumnDisplaySize(15));

        // check getScale()
        assertEquals(0, rsmeta.getScale(1));
        assertEquals(0, rsmeta.getScale(2));
        assertEquals(0, rsmeta.getScale(3));
        assertEquals(0, rsmeta.getScale(4));
        assertEquals(0, rsmeta.getScale(5));
        assertEquals(0, rsmeta.getScale(6));
        assertEquals(0, rsmeta.getScale(7));
        assertEquals(0, rsmeta.getScale(8));
        assertEquals(0, rsmeta.getScale(9));
        assertEquals(0, rsmeta.getScale(10));
        assertEquals(0, rsmeta.getScale(11));
        assertEquals(0, rsmeta.getScale(12));
        assertEquals(0, rsmeta.getScale(13));
        assertEquals(0, rsmeta.getScale(14));
        assertEquals(5, rsmeta.getScale(15));

        // check getPrecision
        assertEquals(0, rsmeta.getPrecision(1));
        assertEquals(0, rsmeta.getPrecision(2));
        assertEquals(0, rsmeta.getPrecision(3));
        assertEquals(0, rsmeta.getPrecision(4));
        assertEquals(0, rsmeta.getPrecision(5));
        assertEquals(0, rsmeta.getPrecision(6));
        assertEquals(0, rsmeta.getPrecision(7));
        assertEquals(0, rsmeta.getPrecision(8));
        assertEquals(0, rsmeta.getPrecision(9));
        assertEquals(0, rsmeta.getPrecision(10));
        assertEquals(0, rsmeta.getPrecision(11));
        assertEquals(0, rsmeta.getPrecision(12));
        assertEquals(0, rsmeta.getPrecision(13));
        assertEquals(0, rsmeta.getPrecision(14));
        assertEquals(20, rsmeta.getPrecision(15));
    }

    @Test
    public void getColumnTypeTest() throws SQLException {
        String createTable = "create or replace table col_types_test" +
                "(col1 bool, col2 tinyint, col3 smallint, col4 int, col5 bigint, col6 real, col7 double, " +
                "col8 varchar(10), col9 nvarchar(10), col10 text(10), col11 date, col12 datetime)";
        String select = "select * from col_types_test";

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate(createTable);
            ResultSet rs = stmt.executeQuery(select);
            ResultSetMetaData metaData = rs.getMetaData();

            assertEquals(Types.BOOLEAN, metaData.getColumnType(1));
            assertEquals(Types.TINYINT, metaData.getColumnType(2));
            assertEquals(Types.SMALLINT, metaData.getColumnType(3));
            assertEquals(Types.INTEGER, metaData.getColumnType(4));
            assertEquals(Types.BIGINT, metaData.getColumnType(5));
            assertEquals(Types.REAL, metaData.getColumnType(6));
            assertEquals(Types.DOUBLE, metaData.getColumnType(7));
            assertEquals(Types.VARCHAR, metaData.getColumnType(8));
            assertEquals(Types.NVARCHAR, metaData.getColumnType(9));
            assertEquals(Types.NVARCHAR, metaData.getColumnType(10));
            assertEquals(Types.DATE, metaData.getColumnType(11));
            assertEquals(Types.TIMESTAMP, metaData.getColumnType(12));

        }
    }
}
