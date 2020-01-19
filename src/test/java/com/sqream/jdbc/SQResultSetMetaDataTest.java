package com.sqream.jdbc;

import org.junit.Test;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class SQResultSetMetaDataTest {

    @Test
    public void isCurrencyTest() throws Connector.ConnException, SQLException, IOException, NoSuchAlgorithmException, ScriptException, KeyManagementException {
        String catalog = "master";
        Connector connector = new Connector("127.0.0.1", 5000, false, false);

        // SQ-1246: JDBC - Set default value to isCurrency
        // check that return false by default
        SQResultSetMetaData resultSetMetaData = new SQResultSetMetaData(connector, catalog);

        assertFalse(resultSetMetaData.isCurrency(1));
    }

}