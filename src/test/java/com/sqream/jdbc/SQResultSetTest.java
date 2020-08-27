package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

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

}
