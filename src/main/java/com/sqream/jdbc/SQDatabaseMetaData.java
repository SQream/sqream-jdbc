/**
 * 
 */
package com.sqream.jdbc;


import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;

import javax.script.ScriptException;

import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorImpl;
import com.sqream.jdbc.connector.ConnectorImpl.ConnException;


/**
 * @author root
 * 
 */

public class SQDatabaseMetaData implements DatabaseMetaData {

	Path SQDatabaseMetaData_log = Paths.get("/tmp/SQDatabaseMetaData.txt");
	boolean log(String line) throws SQLException {
		try {
			Files.write(SQDatabaseMetaData_log, Arrays.asList(new String[] {line}), UTF_8, CREATE, APPEND);
		} catch (IOException e) {
			e.printStackTrace();
			throw new SQLException ("Error writing to SQDatabaseMetaData log");
		}
		
		return true;
	}
	
	private Connector client;
	private SQConnection conn;
	private String user;
	private String databaseProductName = "SqreamDB";
	private String databaseProductVersion = "";
	private int databaseMajorVersion = 0;
	private int databaseMinorVersion = 0;
	private int driverMajorVersion = 4;
	private int driverMinorVersion = 0;
	private String driverVersion = "4.0";
	private String dbName;
	
	static void print(Object printable) {
        System.out.println(printable);
    }
	
	public SQDatabaseMetaData(Connector client, SQConnection conn, String user_, String catalog) throws SQLException, NumberFormatException, UnknownHostException, IOException
			 {
		this.client = client;
		this.conn =conn;
		user = user_;
		dbName = catalog;
	}
	
	SQResultSet metadataStatement(String sql) throws ConnException, IOException, SQLException, ScriptException, NoSuchAlgorithmException, KeyManagementException {

		Connector client = new ConnectorImpl(conn.getParams().getIp(), conn.getParams().getPort(), conn.getParams().getCluster(), conn.getParams().getUseSsl());
		client.connect(conn.getParams().getDbName(), conn.getParams().getUser(), conn.getParams().getPassword(), conn.getParams().getService());
		client.execute(sql);
		
		return new SQResultSet(client, dbName, true);
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {

		try {
			return metadataStatement("select get_catalogs()");
		}
		catch (IOException | ConnException | ScriptException | NoSuchAlgorithmException | KeyManagementException e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
		
	}
	
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {

		String sql = "select get_columns(" + CheckNull(catalog) + "," + CheckNull(schemaPattern) + "," + CheckNull(tableNamePattern) + ",'*')";
		sql = sql.toLowerCase();
		try {
			return metadataStatement(sql);
		}
		
		catch (IOException | ConnException | ScriptException | NoSuchAlgorithmException | KeyManagementException e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
	}
	
	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
		ResultSet rs = null;
		try {
			rs = metadataStatement("select database_name as PROCEDURE_CAT, null as PROCEDURE_SCHEM, function_name as PROCEDURE_NAME, null as UNUSED, null as UNUSED2, null as UNUSED3, ' ' as REMARKS, 0 as PROCEDURE_TYPE, function_name as SPECIFIC_NAME from sqream_catalog.user_defined_functions");	
		} catch (IOException | ConnException | ScriptException | NoSuchAlgorithmException | KeyManagementException e) {
			e.printStackTrace();
		}
		
		return rs;
	}
	
	@Override
	public ResultSet getSchemas() throws SQLException {
		
		try {
			return metadataStatement("select get_schemas()");
		}
		
		catch (IOException | ConnException | ScriptException | NoSuchAlgorithmException | KeyManagementException e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
	}
	
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
		
		String logTypes = "";
		if (types != null && types.length > 0) {
			for (String type : types) {
				logTypes += type + ";";
			}
		}

		// We currently support only tables and views, so see if any of them
		// were reqeusted.
		String strTypes = "";
		if (types != null && types.length > 0) {
			boolean bFoundView = false;
			boolean bFoundTable = false;
			for (String type : types) {
				if (type.contains("TABLE")) {
					bFoundTable = true;
					if (strTypes != "")
						strTypes += ",";
					strTypes += "table";
				} else if (type.contains("VIEW")) {
					bFoundView = true;
					if (strTypes != "")
						strTypes += ",";
					strTypes += "view";
				}
			}

			if (!bFoundView && !bFoundTable) {
				throw new SQLException("error", "Got " + logTypes
						+ " and couldn't find TABLE or VIEW");
			}
		} else {
			strTypes = "*";
		}
		String sql = "select get_tables(" + CheckNull(catalog) + ","
				+ CheckNull(schemaPattern) + ",'*'," + CheckNull(strTypes)
				+ ")";
		try {
			return metadataStatement(sql);
		} catch (IOException | ConnException | ScriptException | NoSuchAlgorithmException | KeyManagementException e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		
		return "curdate, curtime, dayname, dayofmonth, dayofweek, dayofyear, hour, minute, month, monthname, now, quarter, timestampadd, timestampdiff, second, week, year";
	}
	
	@Override
	public String getStringFunctions() throws SQLException {
		// Retrieves a comma-separated list of string functions available with this database. 
		// These are the Open Group CLI string function names used in the JDBC function escape clause.
	
		return "";
	}
	

	@Override
	public String getSystemFunctions() throws SQLException {
		// Retrieves a comma-separated list of system functions available with this database
		return "";
	}
	
	
	@Override
	public String getNumericFunctions() throws SQLException {
		//Retrieves a comma-separated list of math functions available with this database.
		
		return "";
	}
	
	
	@Override
	public ResultSet getTypeInfo() throws SQLException {

		try {
			return metadataStatement("select get_type_info()");
		} catch (IOException | ConnException | ScriptException | NoSuchAlgorithmException | KeyManagementException e) {
			throw new SQLException(e.getMessage());
		}
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
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

		return String.format("jdbc:Sqream://%s:%s/%s;user=%s;password=%s",
				conn.getParams().getCluster() ? conn.getParams().getLbip() : conn.getParams().getIp(),
				conn.getParams().getCluster() ? conn.getParams().getLbport() : conn.getParams().getPort(),
				conn.getParams().getDbName(),
				conn.getParams().getUser(),
				conn.getParams().getPassword());
	}

	@Override
	public String getUserName() throws SQLException {
		return user;
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
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
		return getSchemas();
	}
	
	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
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
		return new SQConnection(client);
	}
	
	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
			throws SQLException {
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
		// TODO FIX
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
		
		try {
			return metadataStatement("select get_table_types()");
		} catch (IOException | ConnException | ScriptException | NoSuchAlgorithmException | KeyManagementException e) {
			throw new SQLException(e.getMessage());
		}
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
		return databaseMajorVersion;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return databaseMinorVersion;
	}
	
	@Override
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}
	
	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return " ";
	}
	
	@Override
	public String getCatalogTerm() throws SQLException {
		return "database";
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "SqreamDB";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return databaseProductVersion;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return 0;
	}

	@Override
	public int getDriverMajorVersion() {
		return driverMajorVersion;
	}

	@Override
	public int getDriverMinorVersion() {
		return driverMinorVersion;
	}

	@Override
	public String getDriverName() throws SQLException {
		return "com.sqream.jdbc.SQDriver";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return driverVersion;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return 4;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return "function"; // Not supported at the moment
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return 0;
	}
	
	@Override
	public String getSQLKeywords() throws SQLException {
		return "";
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return 0;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "schema";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "";
	}
	

	private String CheckNull(String str) {
		return str == null ? "'*'" : "'" + str.trim() + "'";
	}

	
	
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return true;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}
	
	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
			return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
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
