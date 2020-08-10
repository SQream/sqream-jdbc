package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.sql.SQLWarning;

import static org.junit.Assert.*;

public class SQResultSetTest {

    @Test
    public void callGetWarningsOnResultSetTest() throws  SQLException {
        ConnectorImpl mockConnector = Mockito.mock(ConnectorImpl.class);
        SQResultSet rs = new SQResultSet(mockConnector, "");

        SQLWarning warnings = rs.getWarnings();

        assertNull(warnings);
    }

    @Test(expected = SQLException.class)
    public void callGetWarningsOnClosedResultSetTest() throws  SQLException {
        ConnectorImpl mockConnector = Mockito.mock(ConnectorImpl.class);
        Mockito.when(mockConnector.isOpen()).thenReturn(false);
        SQResultSet rs = new SQResultSet(mockConnector, "");
        rs.close();

        rs.getWarnings();
    }

}
