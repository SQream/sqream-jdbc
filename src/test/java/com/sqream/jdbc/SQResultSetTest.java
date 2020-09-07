package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.*;

import static com.sqream.jdbc.TestEnvironment.URL;
import static com.sqream.jdbc.TestEnvironment.createConnection;
import static org.junit.Assert.*;

public class SQResultSetTest {

    @Test
    public void callGetWarningsOnResultSetTest() throws  SQLException {
        ConnectorImpl mockConnector = Mockito.mock(ConnectorImpl.class);
        SQResultSet rs = SQResultSet.getInstance(mockConnector, "");

        SQLWarning warnings = rs.getWarnings();

        assertNull(warnings);
    }

    @Test(expected = SQLException.class)
    public void callGetWarningsOnClosedResultSetTest() throws  SQLException {
        ConnectorImpl mockConnector = Mockito.mock(ConnectorImpl.class);
        Mockito.when(mockConnector.isOpen()).thenReturn(false);
        SQResultSet rs = SQResultSet.getInstance(mockConnector, "");
        rs.close();

        rs.getWarnings();
    }

    @Test
    public void callClearWarningsOnResultSetTest() throws  SQLException {
        ConnectorImpl mockConnector = Mockito.mock(ConnectorImpl.class);
        SQResultSet rs = SQResultSet.getInstance(mockConnector, "");

        rs.clearWarnings();
    }

    @Test(expected = SQLException.class)
    public void callClearWarningsOnClosedResultSetTest() throws  SQLException {
        ConnectorImpl mockConnector = Mockito.mock(ConnectorImpl.class);
        Mockito.when(mockConnector.isOpen()).thenReturn(false);
        SQResultSet rs = SQResultSet.getInstance(mockConnector, "");
        rs.close();

        rs.clearWarnings();
    }

    @Test
    public void getInstanceTest() throws SQLException {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select 1;")) {

            assertEquals(SQResultSet.class, rs.getClass());
        }

        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            assertTrue(stmt.execute("select 1;"));
            assertEquals(SQResultSet.class, stmt.getResultSet().getClass());
        }

        try (Connection conn = DriverManager.getConnection(URL + ";loggerLevel=debug");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select 1;")) {

            assertEquals(SQLoggableResultSet.class, rs.getClass());
        }

        try (Connection conn = DriverManager.getConnection(URL + ";loggerLevel=debug");
             Statement stmt = conn.createStatement()) {

            assertTrue(stmt.execute("select 1;"));
            assertEquals(SQLoggableResultSet.class, stmt.getResultSet().getClass());
        }

        try (Connection conn = DriverManager.getConnection(URL + ";loggerLevel=trace");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select 1;")) {

            assertEquals(SQLoggableResultSet.class, rs.getClass());
        }

        try (Connection conn = DriverManager.getConnection(URL + ";loggerLevel=trace");
             Statement stmt = conn.createStatement()) {

            assertTrue(stmt.execute("select 1;"));
            assertEquals(SQLoggableResultSet.class, stmt.getResultSet().getClass());
        }
    }
}
