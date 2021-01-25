package com.sqream.jdbc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.script.ScriptException;

import com.sqream.jdbc.catalogQueryBuilder.CatalogQueryBuilder;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.connector.ConnException;

public class SQDatabaseMetaData implements DatabaseMetaData {
	private static final Logger LOGGER = Logger.getLogger(SQDatabaseMetaData.class.getName());

	private Connector client;
	private SQConnection conn;
	private String user;
	private String databaseProductName = "SqreamDB";
	private String databaseProductVersion = "";
	private int databaseMajorVersion = 0;
	private int databaseMinorVersion = 0;
	private int driverMajorVersion = 4;
	private int driverMinorVersion = 0;
	private String driverVersion = "4.3.2";
	private String dbName;
	private final ConnectorFactory connectorFactory;
	private final CatalogQueryBuilder catalogQueryBuilder;

	public SQDatabaseMetaData(Connector client,
							  SQConnection conn,
							  String user,
							  String catalog,
							  ConnectorFactory connectorFactory,
							  CatalogQueryBuilder catalogQueryBuilder) {
		this.client = client;
		this.conn = conn;
		this.user = user;
		this.dbName = catalog;
		this.connectorFactory = connectorFactory;
		this.catalogQueryBuilder = catalogQueryBuilder;
	}

	SQResultSet metadataStatement(String sql) throws SQLException {

		try {
			Connector client = connectorFactory.createConnector(conn.getParams());
			client.connect(conn.getParams().getDbName(), conn.getParams().getUser(), conn.getParams().getPassword(), conn.getParams().getService());
			client.execute(sql);

			return SQResultSet.getInstance(client, dbName);
		} catch (ConnException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		LOGGER.log(Level.FINE, "Get catalogs from database meta data.");
		return metadataStatement(catalogQueryBuilder.getCatalogs());
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get columns: catalog=[{0}], schemaPattern=[{1}], tableNamePattern=[{2}], columnNamePattern=[{3}]",
				catalog, schemaPattern, tableNamePattern, columnNamePattern));
		String sql = catalogQueryBuilder.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern, dbName);
		return metadataStatement(sql);
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get procedures: catalog=[{0}], schemaPattern=[{1}], procedureNamePattern=[{2}]",
				catalog, schemaPattern, procedureNamePattern));

		return metadataStatement(catalogQueryBuilder.getProcedures());
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		LOGGER.log(Level.FINE, "Get schemas");
		return metadataStatement(catalogQueryBuilder.getSchemas());
	}

	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get tables: catalog=[{0}], schemaPattern=[{1}], tableNamePattern=[{2}], types=[{3}])",
				catalog, schemaPattern, tableNamePattern, types));

		String sql = catalogQueryBuilder.getTables(catalog, schemaPattern, tableNamePattern, types, dbName);
		return metadataStatement(sql);
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		LOGGER.log(Level.FINE, "Get time date functions");
		return "curdate, curtime, dayname, dayofmonth, dayofweek, dayofyear, hour, minute, month, monthname, now, quarter, timestampadd, timestampdiff, second, week, year";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		// Retrieves a comma-separated list of string functions available with this database.
		// These are the Open Group CLI string function names used in the JDBC function escape clause.
		LOGGER.log(Level.FINE, "Get string functions");
		return "CHAR_LENGTH, CHARINDEX, ||, ISPREFIXOF, LEFT, LEN, LIKE, LOWER, LTRIM, OCTET_LENGTH, PATINDEX, REGEXP_COUNT, REGEXP_INSTR, REGEXP_SUBSTR, REPLACE, REVERSE, RIGHT, RLIKE, RTRIM, SUBSTRING, TRIM, LOWER";
	}


	@Override
	public String getSystemFunctions() throws SQLException {
		// Retrieves a comma-separated list of system functions available with this database
		LOGGER.log(Level.FINE, "Get system functions");
		return "explain, show_connections, show_locks, show_node_info, show_server_status, show_version, stop_statement";
	}


	@Override
	public String getNumericFunctions() throws SQLException {
		//Retrieves a comma-separated list of math functions available with this database.
		LOGGER.log(Level.FINE, "Get numeric functions");
		return "ABS, ACOS, ASIN, ATAN, ATN2, CEILING, CEIL, COS, COT, CRC64, DEGREES, EXP, FLOOR, LOG, LOG10, MOD, %, PI, POWER, RADIANS, ROUND, SIN, SQRT, SQUARE, TAN, TRUNC";
	}


	@Override
	public ResultSet getTypeInfo() throws SQLException {
		LOGGER.log(Level.FINE, "Get type info");
		return metadataStatement(catalogQueryBuilder.getTypeInfo());
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get UDTs: catalog=[{0}], schemaPattern=[{1}], typeNamePattern=[{2}], int[] types",
				catalog, schemaPattern, typeNamePattern, types));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return rs;
	}

	@Override
	public String getURL() throws SQLException {
		String result = String.format("jdbc:Sqream://%s:%s/%s;user=%s;password=%s",
				conn.getParams().getIp(),
				conn.getParams().getPort(),
				conn.getParams().getDbName(),
				conn.getParams().getUser(),
				conn.getParams().getPassword());
		LOGGER.log(Level.FINE, MessageFormat.format("Get URL returns [{0}]", result));
		return result;
	}

	@Override
	public String getUserName() throws SQLException {
		LOGGER.log(Level.FINE, "Get user name");
		return user;
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get version columns: catalog=[{0}], schema=[{1}], table=[{2}]", catalog, schema, table));
		ResultSet rs = null;
		try {
			String[] lables = { "catalog", "schema", "table" };
			String[] values = { catalog, schema, table };

			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// SQResultSet rs = new SQResultSet(CreateVersionColumnsMetaData());
		// rs.MetaDataResultSet = true; // The Hack.
		return rs;
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get version columns: catalog=[{0}], schemaPattern=[{1}], typeNamePattern=[{2}], attributeNamePattern=[{3}]",
				catalog, schemaPattern, typeNamePattern, attributeNamePattern));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get best row identifier: catalog=[{0}], schema=[{1}], table=[{2}], scope=[{3}], nullable=[{4}]",
				catalog, schema, table, scope, nullable));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}


	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get schemas: catalog=[{0}], schemaPattern=[{1}]", catalog, schemaPattern));
		return getSchemas();
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		LOGGER.log(Level.FINE, "Get client info properties");
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("Get column privileges: catalog=[{0}], schema=[{1}], table=[{2}], columnNamePattern=[{3}]",
				catalog, schema, table, columnNamePattern));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
		// SQResultSet rs = new SQResultSet(CreateColumnPrivilegesMetaData());
		// rs.MetaDataResultSet = true; // The Hack.

	}

	@Override
	public Connection getConnection() {
		LOGGER.log(Level.FINE, "Get connection");
		return new SQConnection(client);
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
			throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get function columns: catalog=[{0}], schemaPattern=[{1}], functionNamePattern=[{2}],  columnNamePattern=[{3}]",
				catalog, schemaPattern, functionNamePattern, columnNamePattern));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get functions: catalog=[{0}], schemaPattern=[{1}], functionNamePattern=[{2}]",
				catalog, schemaPattern, functionNamePattern));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get imported keys: catalog=[{0}], schema=[{1}], table=[{2}]", catalog, schema, table));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get index info: catalog=[{0}], schema=[{1}], table=[{2}],  unique=[{3}], approximate=[{4}]",
				catalog, schema, table, unique, approximate));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema,
			String foreignTable) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get cross reference: parentCatalog=[{0}], parentSchema=[{1}], parentTable=[{2}],  " +
						"foreignCatalog=[{3}], foreignSchema=[{4}], foreignTable=[{5}]",
				parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get exported keys: catalog=[{0}], schema=[{1}], table=[{2}]", catalog, schema, table));
		ResultSet rs = null;
		try {
			String[] lables = { "catalog", "schema", "table" };
			String[] values = { catalog, schema, table };

			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// SQResultSet rs = new SQResultSet(CreateKeysMetaData());
		// rs.MetaDataResultSet = true; // The Hack.

		// return rs;

		return rs;
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get primary keys: catalog=[{0}], schema=[{1}], table=[{2}]", catalog, schema, table));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return rs;
		// SQResultSet rs = new SQResultSet(CreatePrimaryKeysMetaData());
		// rs.MetaDataResultSet = true; // The Hack.

		// return rs;
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
			throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get procedure columns: catalog=[{0}], schemaPattern=[{1}], procedureNamePattern=[{2}], columnNamePattern=[{3}]",
				catalog, schemaPattern, procedureNamePattern, columnNamePattern));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return rs;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get super types: catalog=[{0}], schemaPattern=[{1}], typeNamePattern=[{2}]",
				catalog, schemaPattern, typeNamePattern));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return rs;
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Get table privileges: catalog=[{0}], schemaPattern=[{1}], tableNamePattern=[{2}]",
				catalog, schemaPattern, tableNamePattern));
		ResultSet rs = null;
		try {
			rs = EmptyResultSet();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return rs;
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return metadataStatement(catalogQueryBuilder.getTableTypes());
	}

	// Replace this SQL with a real empty result set !
	private ResultSet EmptyResultSet() throws SQLException, IOException {

		// Omer, related to BG-769 - boost performance by not using the
		// statement
		// and instead returns an empty ResultSet.
		// return execute("select top 0 * from sqream_catalog.tables");
		return new SQResultSet(true); // empty
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return databaseMajorVersion;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return databaseMinorVersion;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return ".";
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return "\"";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return "database";
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return "SqreamDB";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return databaseProductVersion;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getDriverMajorVersion() {
		LOGGER.log(Level.FINE, "method called");
		return driverMajorVersion;
	}

	@Override
	public int getDriverMinorVersion() {
		LOGGER.log(Level.FINE, "method called");
		return driverMinorVersion;
	}

	@Override
	public String getDriverName() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return "com.sqream.jdbc.SQDriver";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return driverVersion;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 4;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 128;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 128;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 128;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 128;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return "function"; // Not supported at the moment
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return "";
	}

	@Override
	public int getSQLStateType() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return 0;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return "schema";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return "\\";
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return true;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return true;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return true;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return true;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return true;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return true;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return true;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		LOGGER.log(Level.FINE, "method called");
		return false;
	}


	// Unsupported
	// -----------

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("unwrap in SQDatabaseMetadata");
	}

	@Override
	public ResultSet getPseudoColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		throw new SQLFeatureNotSupportedException("getPseudoColumns in SQDatabaseMetadata");
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		throw new SQLFeatureNotSupportedException("getExtraNameCharacters in SQDatabaseMetadata");
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw new SQLFeatureNotSupportedException("getRowIdLifetime in SQDatabaseMetadata");
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("getSuperTables in SQDatabaseMetadata");
		// return EmptyResultSet();
	}
}
