package com.sqream.jdbc;

import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class SQDatabaseMetaDataTest {
    private static SQDatabaseMetaData metadata;

    @Before
    public void setUp() throws KeyManagementException, ScriptException, NoSuchAlgorithmException,
            Connector.ConnException, IOException, SQLException {

        String user = "sqream";
        String catalog = "master";
        Connector connector = new Connector("127.0.0.1", 5000, false, false);
        SQConnection connection = new SQConnection(connector);
        metadata = new SQDatabaseMetaData(connector, connection, user, catalog);
    }

    @Test
    public void storesLowerCaseIdentifiersTest() throws IOException, SQLException, ScriptException, NoSuchAlgorithmException, Connector.ConnException, KeyManagementException {
        assertNotNull(metadata);
        assertTrue(metadata.storesLowerCaseIdentifiers());
    }

    @Test
    public void storesLowerCaseQuotedIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.storesLowerCaseQuotedIdentifiers());
    }

    @Test
    public void storesUpperCaseIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.storesUpperCaseIdentifiers());
    }

    @Test
    public void storesUpperCaseQuotedIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.storesUpperCaseQuotedIdentifiers());
    }

    @Test
    public void storesMixedCaseIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.storesMixedCaseIdentifiers());
    }

    @Test
    public void storesMixedCaseQuotedIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertTrue(metadata.storesMixedCaseQuotedIdentifiers());
    }

    @Test
    public void supportsMixedCaseIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.supportsMixedCaseIdentifiers());
    }

    @Test
    public void supportsMixedCaseQuotedIdentifiersTest() throws SQLException {
        assertNotNull(metadata);
        assertTrue(metadata.supportsMixedCaseQuotedIdentifiers());
    }

    @Test
    public void supportsBatchUpdatesTest() throws SQLException {
        assertNotNull(metadata);
        assertTrue(metadata.supportsBatchUpdates());
    }

    @Test
    public void supportsCatalogsInDataManipulationTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.supportsCatalogsInDataManipulation());
    }

    @Test
    public void supportsSchemasInDataManipulationTest() throws SQLException {
        assertNotNull(metadata);
        assertFalse(metadata.supportsSchemasInDataManipulation());
    }
}