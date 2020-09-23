package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Test;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

public class SQResultSetMetaDataTest {

    @Test
    public void isCurrencyTest() throws
            ConnException, NoSuchAlgorithmException, IOException, KeyManagementException, SQLException {

        Connector connector = new ConnectorImpl(
                ConnectionParams.builder()
                        .ipAddress(IP)
                        .port(String.valueOf(PORT))
                        .cluster(String.valueOf(CLUSTER))
                        .useSsl(String.valueOf(SSL))
                        .build());

        SQResultSetMetaData resultSetMetaData = new SQResultSetMetaData(connector, DATABASE);

        assertFalse(resultSetMetaData.isCurrency(1));
    }

    @Test
    public void columnDisplaySizeTest() throws SQLException {
        String createSql = "create or replace table test_display_size " +
                "(col1 bool, col2 tinyint, col3 smallint, col4 int, col5 bigint, col6 real, col7 double, " +
                "col8 varchar(10), col9 nvarchar(10), col10 text(10), col11 nvarchar, col12 text, col13 date, col14 datetime)";
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
