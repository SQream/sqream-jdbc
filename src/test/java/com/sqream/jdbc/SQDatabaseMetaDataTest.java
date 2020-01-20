package com.sqream.jdbc;

import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import static com.sqream.jdbc.TestEnvironment.*;
import static org.junit.Assert.*;

public class SQDatabaseMetaDataTest {
    private static SQDatabaseMetaData metadata;

    @Before
    public void setUp() throws KeyManagementException, NoSuchAlgorithmException, IOException, SQLException {

        Connector connector = new ConnectorImpl(IP, PORT, CLUSTER, SSL);
        SQConnection connection = new SQConnection(connector);
        metadata = new SQDatabaseMetaData(connector, connection, USER, DATABASE);
    }

    @Test
    public void storesLowerCaseIdentifiersTest() throws SQLException {
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