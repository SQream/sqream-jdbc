package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Test;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

public class SQResultSetMetaDataTest {

    @Test
    public void isCurrencyTest() throws
            ConnException, NoSuchAlgorithmException, IOException, KeyManagementException, SQLException {

        Connector connector = new ConnectorImpl(IP, PORT, CLUSTER, SSL);

        SQResultSetMetaData resultSetMetaData = new SQResultSetMetaData(connector, DATABASE);

        assertFalse(resultSetMetaData.isCurrency(1));
    }
}