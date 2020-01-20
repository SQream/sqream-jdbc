package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Test;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class SQResultSetMetaDataTest {

    @Test
    public void isCurrencyTest() throws
            ConnException, NoSuchAlgorithmException, IOException, KeyManagementException, SQLException {

        String catalog = "master";
        Connector connector = new ConnectorImpl("127.0.0.1", 5000, false, false);

        SQResultSetMetaData resultSetMetaData = new SQResultSetMetaData(connector, catalog);

        assertFalse(resultSetMetaData.isCurrency(1));
    }
}