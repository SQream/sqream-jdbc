package com.sqream.jdbc;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.*;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

import static com.sqream.jdbc.TestEnvironment.*;
import static com.sqream.jdbc.TestEnvironment.DATABASE;
import static com.sqream.jdbc.TestEnvironment.DATABASE;
import static com.sqream.jdbc.TestEnvironment.USER;
import static com.sqream.jdbc.TestUtils.createConnectionParams;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class SQDatabaseMetaDataTest {
    private static SQDatabaseMetaData metadata;

    @Mock
    private ConnectorFactory connectorFactoryMock;
    @Mock
    private Connector connectorMock;
    @Mock
    SQConnection connectionMock;
    @Captor
    private ArgumentCaptor<String> connectorExecuteCaptor;

    @Before
    public void setUp() throws ConnException {

        Connector connector = new ConnectorImpl(
                ConnectionParams.builder()
                        .ipAddress(IP)
                        .port(String.valueOf(PORT))
                        .cluster(String.valueOf(CLUSTER))
                        .useSsl(String.valueOf(SSL))
                        .build());
        SQConnection connection = new SQConnection(connector);
        metadata = new SQDatabaseMetaData(connector, connection, USER, DATABASE, new ConnectorFactory());
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

    @Test
    public void whenCatalogEmptyGetEmptyResultTest() throws SQLException {
        String CATALOG = "";
        Set<String> columnsByQuery = new HashSet<>();
        Set<String> columnsFromMetadata = new HashSet<>();

        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format("select get_columns('%s', '*', '*', '*');", CATALOG));
                while (rs.next()) {
                    columnsByQuery.add(rs.getString(3));
                }
            }
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(CATALOG, null, null, null);
            while (rs.next()) {
                columnsFromMetadata.add(rs.getString(3));
            }
        }

        assertEquals(columnsByQuery.size(), columnsFromMetadata.size());
        for (String tableFromQuery : columnsByQuery) {
            assertTrue(columnsFromMetadata.contains(tableFromQuery));
        }
    }

    @Test
    public void whenCatalogNullGetCurrentDatabaseColumnsTest() throws SQLException {
        Set<String> columnsByQuery = new HashSet<>();
        Set<String> columnsFromMetadata = new HashSet<>();

        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format("select get_columns('%s', '*', '*', '*');", DATABASE));
                while (rs.next()) {
                    columnsByQuery.add(rs.getString(3));
                }
            }
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(null, null, null, null);
            while (rs.next()) {
                columnsFromMetadata.add(rs.getString(3));
            }
        }

        assertEquals(columnsByQuery.size(), columnsFromMetadata.size());
        for (String tableFromQuery : columnsByQuery) {
            assertTrue(columnsFromMetadata.contains(tableFromQuery));
        }
    }

    @Test
    public void whenCatalogProvidedGetCurrentDatabaseColumnsTest() throws SQLException {
        Set<String> columnsByQuery = new HashSet<>();
        Set<String> columnsFromMetadata = new HashSet<>();

        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format("select get_columns('%s', '*', '*', '*');", DATABASE));
                while (rs.next()) {
                    columnsByQuery.add(rs.getString(3));
                }
            }
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(DATABASE, null, null, null);
            while (rs.next()) {
                columnsFromMetadata.add(rs.getString(3));
            }
        }

        assertEquals(columnsByQuery.size(), columnsFromMetadata.size());
        for (String tableFromQuery : columnsByQuery) {
            assertTrue(columnsFromMetadata.contains(tableFromQuery));
        }
    }

    @Test
    public void whenCatalogEmptyGetCurrentDatabaseTablesTest() throws SQLException {
        String CATALOG = "";
        Set<String> tablesByQuery = new HashSet<>();
        Set<String> tablesFromMetadata = new HashSet<>();

        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format("select get_tables('%s', '*', '*', '*');", CATALOG));
                while (rs.next()) {
                    tablesByQuery.add(rs.getString(3));
                }
            }
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(CATALOG, null, null, null);
            while (rs.next()) {
                tablesFromMetadata.add(rs.getString(3));
            }
        }

        assertEquals(tablesByQuery.size(), tablesFromMetadata.size());
        for (String tableFromQuery : tablesByQuery) {
            assertTrue(tablesFromMetadata.contains(tableFromQuery));
        }
    }

    @Test
    public void whenCatalogNullGetCurrentDatabaseTablesTest() throws SQLException {
        Set<String> tablesByQuery = new HashSet<>();
        Set<String> tablesFromMetadata = new HashSet<>();

        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format("select get_tables('%s', '*', '*', '*');", DATABASE));
                while (rs.next()) {
                    tablesByQuery.add(rs.getString(3));
                }
            }
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, null, null, null);
            while (rs.next()) {
                tablesFromMetadata.add(rs.getString(3));
            }
        }

        assertEquals(tablesByQuery.size(), tablesFromMetadata.size());
        for (String tableFromQuery : tablesByQuery) {
            assertTrue(tablesFromMetadata.contains(tableFromQuery));
        }
    }

    @Test
    public void whenCatalogProvidedGetCurrentDatabaseTablesTest() throws SQLException {
        Set<String> tablesByQuery = new HashSet<>();
        Set<String> tablesFromMetadata = new HashSet<>();

        try (Connection conn = createConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format("select get_tables('%s', '*', '*', '*');", DATABASE));
                while (rs.next()) {
                    tablesByQuery.add(rs.getString(3));
                }
            }
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(DATABASE, null, null, null);
            while (rs.next()) {
                tablesFromMetadata.add(rs.getString(3));
            }
        }

        assertEquals(tablesByQuery.size(), tablesFromMetadata.size());
        for (String tableFromQuery : tablesByQuery) {
            assertTrue(tablesFromMetadata.contains(tableFromQuery));
        }
    }

    @Test
    public void getCatalogsTest() throws ConnException, SQLException {
        String EXPECTED_GET_CATALOGS_QUERY = "select get_catalogs()";
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData = new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock);
        metaData.getCatalogs();

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_CATALOGS_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void getProceduresTest() throws ConnException, SQLException {
        String EXPECTED_GET_PROCEDURES_QUERY = "select database_name as PROCEDURE_CAT, null as PROCEDURE_SCHEM, " +
                "function_name as PROCEDURE_NAME, null as UNUSED, null as UNUSED2, null as UNUSED3, ' ' as REMARKS, " +
                "0 as PROCEDURE_TYPE, function_name as SPECIFIC_NAME from sqream_catalog.user_defined_functions";
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock);
        metaData.getProcedures(null, null, null);

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_PROCEDURES_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void getSchemasTest() throws ConnException, SQLException {
        String EXPECTED_GET_SCHEMAS_QUERY = "select get_schemas()";
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock);
        metaData.getSchemas();

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_SCHEMAS_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void getTablesTest() throws ConnException, SQLException {
        String DATABASE = "testDatabase";
        String SCHEMA_PATTERN = "testSchemaPattern";
        String TABLE_NAME_PATTERN = "testTableNamePattern";
        String[] TYPES = new String[]{"TABLE", "VIEW"};
        String EXPECTED_GET_TABLES_QUERY = String.format("select get_tables('%s','%s','*','%s,%s')",
                DATABASE, SCHEMA_PATTERN, TYPES[0].toLowerCase(), TYPES[1].toLowerCase());
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock);
        metaData.getTables(DATABASE, SCHEMA_PATTERN, TABLE_NAME_PATTERN, TYPES);

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_TABLES_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void getTypeInfoTest() throws ConnException, SQLException {
        String EXPECTED_GET_TYPE_INFO_QUERY = "select get_type_info()";
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock);
        metaData.getTypeInfo();

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_TYPE_INFO_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void getUDTsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getUDTs(null, null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getURLTest() throws SQLException {
        ConnectionParams params = createConnectionParams();
        String expectedUrl = String.format("jdbc:Sqream://%s:%s/%s;user=%s;password=%s",
                params.getIp(),
                params.getPort(),
                params.getDbName(),
                params.getUser(),
                params.getPassword());
        Mockito.when(connectionMock.getParams()).thenReturn(params);

        SQDatabaseMetaData metaData = new SQDatabaseMetaData(null, connectionMock, null, null, null);

        assertEquals(expectedUrl, metaData.getURL());
    }

    @Test
    public void getUserName() throws SQLException {
        String expectedUserName = "testUserName";

        SQDatabaseMetaData metaData = new SQDatabaseMetaData(null, null, expectedUserName, null, null);

        assertEquals(expectedUserName, metaData.getUserName());
    }

    @Test
    public void getVersionColumnsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getVersionColumns(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getAttributesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getAttributes(null, null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getBestRowIdentifierReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getBestRowIdentifier(null, null, null, 0, false);

        assertFalse(rs.next());
    }

    @Test
    public void getSchemasWithAttributesTest() throws ConnException, SQLException {
        String EXPECTED_GET_SCHEMAS_QUERY = "select get_schemas()";
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock);
        metaData.getSchemas(null, null);

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_SCHEMAS_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void getClientInfoPropertiesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getClientInfoProperties();

        assertFalse(rs.next());
    }

    @Test
    public void getColumnPrivilegesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getColumnPrivileges(null, null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getFunctionColumnsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getFunctionColumns(null, null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getFunctionsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getFunctions(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getImportedKeysReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getImportedKeys(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getIndexInfoReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getIndexInfo(null, null, null, false, false);

        assertFalse(rs.next());
    }

    @Test
    public void getCrossReferenceReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getCrossReference(null, null, null,
                null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getExportedKeysReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getExportedKeys(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getPrimaryKeysReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getPrimaryKeys(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getProcedureColumnsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getProcedureColumns(null, null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getSuperTypesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getSuperTypes(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getTablePrivilegesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null);
        ResultSet rs = metaData.getTablePrivileges(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getTableTypesTest() throws ConnException, SQLException {
        String EXPECTED_GET_TABLE_TYPES_QUERY = "select get_table_types()";
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock);
        metaData.getTableTypes();

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_TABLE_TYPES_QUERY, connectorExecuteCaptor.getValue());
    }
}
