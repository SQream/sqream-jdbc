package com.sqream.jdbc;

import com.sqream.jdbc.catalogQueryBuilder.CatalogQueryBuilder;
import com.sqream.jdbc.catalogQueryBuilder.ExplicitCatalogQueryBuilder;
import com.sqream.jdbc.catalogQueryBuilder.PatternSupportedCatalogQueryBuilder;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.connector.ConnectorImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.MockitoRule;

import java.sql.*;
import java.util.*;

import static com.sqream.jdbc.TestEnvironment.*;
import static com.sqream.jdbc.TestEnvironment.DATABASE;
import static com.sqream.jdbc.TestEnvironment.USER;
import static com.sqream.jdbc.TestUtils.createConnectionParams;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;

@RunWith(Parameterized.class)
public class SQDatabaseMetaDataTest {
    private static SQDatabaseMetaData metadata;
    private final CatalogQueryBuilder catalogQueryBuilder;
    private final String getTablesQueryTemplate;
    private final String getColumnsQueryTemplate;
    private final String nullReplacement;
    private final String emptyTypeListReplacement;

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();
    @Mock
    private ConnectorFactory connectorFactoryMock;
    @Mock
    private Connector connectorMock;
    @Mock
    SQConnection connectionMock;
    @Captor
    private ArgumentCaptor<String> connectorExecuteCaptor;
    @Parameterized.Parameters
    public static Collection<Object[]> getCatalogQueryBuilder() {
        return Arrays.asList(new Object[][]{
                {new ExplicitCatalogQueryBuilder(),
                        "*",
                        "*",
                        "select get_tables('%s','%s','%s','%s')",
                        "select get_columns('%s','%s','%s','%s')"},
                {new PatternSupportedCatalogQueryBuilder(),
                        "%",
                        "",
                        "select get_tables_like('%s','%s','%s','%s')",
                        "select get_columns_like('%s','%s','%s','%s')"}
        });
    }

    public SQDatabaseMetaDataTest(CatalogQueryBuilder catalogQueryBuilder,
                                  String nullReplacement,
                                  String emptyTypeListReplacement,
                                  String getTablesQueryTemplate,
                                  String getColumnsQueryTemplate) {
        this.catalogQueryBuilder = catalogQueryBuilder;
        this.emptyTypeListReplacement = emptyTypeListReplacement;
        this.nullReplacement = nullReplacement;
        this.getTablesQueryTemplate = getTablesQueryTemplate;
        this.getColumnsQueryTemplate = getColumnsQueryTemplate;
    }

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
        metadata = new SQDatabaseMetaData(connector, connection, USER, DATABASE, new ConnectorFactory(), catalogQueryBuilder);
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
    public void whenCatalogNullGetCurrentDatabaseColumnsTest() throws SQLException, ConnException {
        String EXPECTED_GET_TABLES_QUERY = String.format(
                getColumnsQueryTemplate, DATABASE, nullReplacement, nullReplacement, nullReplacement);
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
        metaData.getColumns(null, null, null, null);

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_TABLES_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void whenCatalogProvidedGetCurrentDatabaseColumnsTest() throws SQLException, ConnException {
        String EXPECTED_GET_TABLES_QUERY = String.format(
                getColumnsQueryTemplate, DATABASE, nullReplacement, nullReplacement, nullReplacement);
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
        metaData.getColumns(DATABASE, null, null, null);

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_TABLES_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void whenCatalogNullGetCurrentDatabaseTablesTest() throws SQLException, ConnException {
        String EXPECTED_GET_TABLES_QUERY = String.format(
                getTablesQueryTemplate, DATABASE, nullReplacement, nullReplacement, emptyTypeListReplacement);
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
        metaData.getTables(null, null, null, null);

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_TABLES_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void whenCatalogProvidedGetItsTablesTest() throws SQLException, ConnException {
        String EXPECTED_GET_TABLES_QUERY = String.format(
                getTablesQueryTemplate, DATABASE, nullReplacement, nullReplacement, emptyTypeListReplacement);
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
        metaData.getTables(DATABASE, null, null, null);

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_TABLES_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void getCatalogsTest() throws ConnException, SQLException {
        String EXPECTED_GET_CATALOGS_QUERY = "select get_catalogs()";
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData = new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
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
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
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
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
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
        String EXPECTED_GET_TABLES_QUERY = String.format(getTablesQueryTemplate,
                DATABASE, SCHEMA_PATTERN, TABLE_NAME_PATTERN, String.join(",", TYPES).toLowerCase());
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
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
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
        metaData.getTypeInfo();

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_TYPE_INFO_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void getUDTsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
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

        SQDatabaseMetaData metaData = new SQDatabaseMetaData(null, connectionMock, null, null, null, null);

        assertEquals(expectedUrl, metaData.getURL());
    }

    @Test
    public void getUserName() throws SQLException {
        String expectedUserName = "testUserName";

        SQDatabaseMetaData metaData = new SQDatabaseMetaData(null, null, expectedUserName, null, null, null);

        assertEquals(expectedUserName, metaData.getUserName());
    }

    @Test
    public void getVersionColumnsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getVersionColumns(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getAttributesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getAttributes(null, null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getBestRowIdentifierReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getBestRowIdentifier(null, null, null, 0, false);

        assertFalse(rs.next());
    }

    @Test
    public void getSchemasWithAttributesTest() throws ConnException, SQLException {
        String EXPECTED_GET_SCHEMAS_QUERY = "select get_schemas()";
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
        metaData.getSchemas(null, null);

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_SCHEMAS_QUERY, connectorExecuteCaptor.getValue());
    }

    @Test
    public void getClientInfoPropertiesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getClientInfoProperties();

        assertFalse(rs.next());
    }

    @Test
    public void getColumnPrivilegesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getColumnPrivileges(null, null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getFunctionColumnsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getFunctionColumns(null, null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getFunctionsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getFunctions(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getImportedKeysReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getImportedKeys(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getIndexInfoReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getIndexInfo(null, null, null, false, false);

        assertFalse(rs.next());
    }

    @Test
    public void getCrossReferenceReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getCrossReference(null, null, null,
                null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getExportedKeysReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getExportedKeys(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getPrimaryKeysReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getPrimaryKeys(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getProcedureColumnsReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getProcedureColumns(null, null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getSuperTypesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getSuperTypes(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getTablePrivilegesReturnsEmptyResultSetTest() throws ConnException, SQLException {
        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, null, null, null, null, null);
        ResultSet rs = metaData.getTablePrivileges(null, null, null);

        assertFalse(rs.next());
    }

    @Test
    public void getTableTypesTest() throws ConnException, SQLException {
        String EXPECTED_GET_TABLE_TYPES_QUERY = "select get_table_types()";
        Mockito.when(connectorFactoryMock.createConnector(any(ConnectionParams.class))).thenReturn(connectorMock);
        Mockito.when(connectionMock.getParams()).thenReturn(createConnectionParams());

        SQDatabaseMetaData metaData =
                new SQDatabaseMetaData(null, connectionMock, USER, DATABASE, connectorFactoryMock, catalogQueryBuilder);
        metaData.getTableTypes();

        Mockito.verify(connectorMock).execute(connectorExecuteCaptor.capture());
        assertEquals(EXPECTED_GET_TABLE_TYPES_QUERY, connectorExecuteCaptor.getValue());
    }
}
