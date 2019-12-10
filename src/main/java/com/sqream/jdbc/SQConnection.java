package com.sqream.jdbc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.script.ScriptException;

import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.connector.ConnectorImpl;
import com.sqream.jdbc.connector.ConnectorImpl.ConnException;

//Logging
import java.nio.file.Path;
import java.nio.file.Paths;


public class SQConnection implements Connection {
	private static final int[] RESULTSET_TYPES =
			new int[]{ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};
	private static final int[] RESULTSET_CONCURRENCY =
			new int[]{ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};
	private static final int[] RESULTSET_HOLDABILITY =
			new int[]{ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT};

	private ConnectorFactory connectorFactory;
	private Path SQConnection_log = Paths.get("/tmp/SQConnection.txt");
	private Vector<SQStatment> Statement_list = new Vector<SQStatment>();
	private Connector globalClient;
	private boolean printouts = false;
	private SQDatabaseMetaData data = null;
	private ConnectionParams params = new ConnectionParams();
	private AtomicBoolean isClosed = new AtomicBoolean(true);
	private String username;
	private Properties connInfo;
	private String dbName;
	private String DEFAULT_SERVICE = "sqream";
    
	SQConnection(Connector client) {
		globalClient = client;
	}
	
	SQConnection(Properties connectionInfo, ConnectorFactory connectorFactory)
			throws ScriptException, SQLException, NumberFormatException, IOException, NoSuchAlgorithmException, KeyManagementException, ConnException {
		log("inside constructor SQConnection");

		this.connectorFactory = connectorFactory;

		connInfo = connectionInfo;
		String cluster = connectionInfo.getProperty("cluster");

		String ipaddress = connectionInfo.getProperty("host");
		if (ipaddress.equals("-1"))
			throw new SQLException("missing host error");

		String s_port = connectionInfo.getProperty("port");
		if (s_port.equals("-1"))
			throw new SQLException("missing port error");
			
		dbName = connectionInfo.getProperty("dbname");
		if (dbName.equals("-1"))
			throw new SQLException("missing database name error");

		String service = connectionInfo.getProperty("service");
		if(service == null || service.equals("-1")) {
			System.out.println ("no service passed, defaulting to sqream");
			service = "sqream";
		}
		
		String schema = connectionInfo.getProperty("schema");
		if(schema == null || schema.equals("-1")) {
			System.out.println ("no schema passed, defaulting to public");
			schema = "public";
		}
		
		String usr = connectionInfo.getProperty("user");
		username = usr;
		if (usr.equals("-1"))
			throw new SQLException("missing user");

		String pswd = connectionInfo.getProperty("password");
		if (pswd.equals("-1"))
			throw new SQLException("missing database name error");

		// Related to bug #541 - skip the picker if we are in cancel state
		String skipPicker = connectionInfo.getProperty("SkipPicker");
		if (skipPicker == null) {
			skipPicker = "false";
		}

		boolean isCluster=cluster.equalsIgnoreCase("true");
		
		boolean useSsl = false;
		String SSL_Connection = connectionInfo.getProperty("ssl");
		if (SSL_Connection == null || SSL_Connection.isEmpty()) {
			useSsl = true;
		}
		else if (SSL_Connection.equalsIgnoreCase("true")){
			useSsl = true;
		}

		globalClient = this.connectorFactory.initConnector(
				ipaddress, Integer.parseInt(s_port), cluster.equalsIgnoreCase("true"), useSsl);
		globalClient.connect(dbName, usr, pswd, service);
		
		params.setCluster(isCluster);
		params.setIp(ipaddress);
		params.setPort(Integer.parseInt(s_port));
		params.setSchema(schema);
		params.setDbName(dbName);
		params.setPassword(pswd);
		params.setUser(usr);
		params.setUseSsl(useSsl);
		params.setService(service);
		isClosed.set(false);
	}

	void removeItem(Statement obj) {
		if(Statement_list.isEmpty()) {
			return;
		}
		Statement_list.remove(obj);
	}

	@Override
	public Statement createStatement() throws SQLException {
		log("inside constructor SQConnection");

		if (isClosed.get()) {
			throw new SQLException(MessageFormat.format("Statement is closed: isClosed=[{0}]", isClosed));
		}

		SQStatment SQS;
		try {
			SQS = new SQStatment(this, dbName, this.connectorFactory);
			Statement_list.addElement(SQS);
		} catch (Exception e) {
			throw new SQLException(e);
		}
		return SQS;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		log("inside constructor SQConnection");

		if (isClosed.get() || !validParams(resultSetType, resultSetConcurrency)) {
			throw new SQLException(MessageFormat.format("Wrong params in createStatement: " +
							"resultSetType=[{0}], resultSetConcurrency=[{1}]",
					resultSetType, resultSetConcurrency));
		}

		SQStatment SQS = null;
		try {
			SQS = new SQStatment(this, dbName, this.connectorFactory);
			Statement_list.addElement(SQS);
		} catch (Exception e) {
			throw new SQLException(e);
		}

		return SQS;
	}

	private static boolean validParams(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
		// Spec: if a database access error occurs, this method is called on a closed connection or
		// the given parameters are not ResultSet constants indicating type, concurrency and holdability
		return Arrays.stream(RESULTSET_TYPES).anyMatch(i -> i == resultSetType) &&
				Arrays.stream(RESULTSET_CONCURRENCY).anyMatch(i -> i == resultSetConcurrency) &&
				Arrays.stream(RESULTSET_HOLDABILITY).anyMatch(i -> i == resultSetHoldability);
	}

	private static boolean validParams(int resultSetType, int resultSetConcurrency) {
		// Spec: if a database access error occurs, this method is called on a closed connection or
		// the given parameters are not ResultSet constants indicating type, concurrency and holdability
		return Arrays.stream(RESULTSET_TYPES).anyMatch(i -> i == resultSetType) &&
				Arrays.stream(RESULTSET_CONCURRENCY).anyMatch(i -> i == resultSetConcurrency);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		log("inside createStatement SQConnection");

		if (isClosed.get() || !validParams(resultSetType, resultSetConcurrency, resultSetHoldability)) {
			throw new SQLException(MessageFormat.format("Wrong params in createStatement: " +
							"resultSetType=[{0}], resultSetConcurrency=[{1}], resultSetHoldability=[{2}]",
					resultSetType, resultSetConcurrency, resultSetHoldability));
		}

		SQStatment SQS;
		try {
			SQS = new SQStatment(this, dbName, this.connectorFactory);
			Statement_list.addElement(SQS);
			
		} catch (Exception e) {
			throw new SQLException(e);
		}

		return SQS;
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		log("inside prepareStatement SQConnection");

		if (isClosed.get()) {
			throw new SQLException(MessageFormat.format("Statement is closed: isClosed=[{0}]", isClosed));
		}

		SQPreparedStatment SQPS = null;
		try {
			SQPS = new SQPreparedStatment(globalClient, sql, this, dbName);
		} catch (KeyManagementException | NoSuchAlgorithmException | IOException | SQLException | ScriptException | ConnException e) {
			throw new SQLException(e);
		} return SQPS;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		log("inside prepareStatement 2 SQConnection");

		if (isClosed.get()) {
			throw new SQLException(MessageFormat.format("Statement is closed: isClosed=[{0}]", isClosed));
		}

		if (printouts) System.out.println("prepareStatement5");
		//spark use this function 
		//sql = sql.replace("\"", "");
		SQPreparedStatment SQPS = null;
		try {
			SQPS = new SQPreparedStatment(globalClient, sql, this, dbName);
		} catch (KeyManagementException | NoSuchAlgorithmException | IOException | ScriptException | ConnException e) {
			e.printStackTrace();
			throw new SQLException(e);
		} 
		return SQPS;
	}
	
	@Override
	public void close() throws SQLException {
		log("inside close SQConnection");
		try {
			if(Statement_list!=null) {
				for(SQStatment item : Statement_list) {
					item.cancel();
				}
				Statement_list.clear();
			}
			if(globalClient !=null && globalClient.isOpen())
				globalClient.closeConnection();      // Closing Connector
			isClosed.set(true);

		} catch (IOException | ScriptException | ConnException e) {
			// TODO Auto-generated catch block
			throw new SQLException(e);
		}
	}
	
	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		log("inside getMetaData SQConnection");
		if (data == null) {
			try {
				data = new SQDatabaseMetaData(globalClient,this, username, dbName);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SQLException(e);
			}
		}
		return data;
	}
	
	@Override
	public boolean isClosed() throws SQLException {
		log("inside isClosed SQConnection");
		return isClosed.get();
	}
	
	@Override
	public void commit() throws SQLException {
		log("inside commit SQConnection");
	}
	
	@Override
	public boolean getAutoCommit() throws SQLException {
		log("inside getAutoCommit SQConnection");
		return true;
	}
	
	@Override
	public String getSchema() {
		return params.getSchema();
	}
	
	@Override
	public String getCatalog() throws SQLException {
		log("inside getCatalog SQConnection");
		return params.getDbName();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		log("inside getClientInfo SQConnection");
		return new Properties();
	}

	@Override
	public int getHoldability() throws SQLException {
		log("inside getHoldability SQConnection");
		return 0;
	}
	
	@Override
	public int getTransactionIsolation() throws SQLException {
		log("inside getTransactionIsolation SQConnection");
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		log("inside getWarnings SQConnection");
		return null; // Returns: the first SQLWarning object or null if there are none
	}
	@Override
	public boolean isReadOnly() throws SQLException {
		log("inside isReadOnly SQConnection");
		return false;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		log("inside isValid SQConnection");
		return globalClient.isOpen();
	}
	
	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		log("inside setAutoCommit SQConnection");
		if (printouts) {
			System.out.println("setAutoCommit");
		}
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		log("inside setCatalog SQConnection");
		// TODO Shirlie
		// / _connectionInfo.put("dbname", catalog);
		if (printouts) System.out.println("setCatalog");
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		try {
			log("inside setClientInfo SQConnection");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (printouts) System.out.println("setCatalog");
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		try {
			log("inside setClientInfo SQConnection");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (printouts) System.out.println("setClientInfo2");
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		/* Puts this connection in read-only mode as a hint 
		 * to the driver to enable database optimizations.  */
		log("inside setReadOnly SQConnection");
		if (printouts) System.out.println("setReadOnly");
		//throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public int getNetworkTimeout() throws SQLException {
		log("inside getNetworkTimeout SQConnection");
		if (printouts) System.out.println("getNetworkTimeout");
		return 0;
	}
	
	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		log("inside setNetworkTimeout SQConnection");
		if (printouts) {
			System.out.println("setNetworkTimeout");
		}
	}
	
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		log("inside isWrapperFor SQConnection");
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		log("inside unwrap SQConnection");
		throw new SQLFeatureNotSupportedException("unwrap in SQConnection");
	}

	@Override
	public void clearWarnings() throws SQLException {
		log("inside clearWarnings SQConnection");
//		System.out.println("clearWarnings");
	}

	private boolean log(String line) throws SQLException {
		//FIXME: Alex K 08.12.19: replace log function with logger
		return true;
	}

	public ConnectionParams getParams() {
		return params;
	}

	void setParams(ConnectionParams params) {
		this.params = params;
	}

	// Unsupported
	// -----------
	
	@Override
	public void setHoldability(int holdability) throws SQLException {
		log("inside setHoldability SQConnection");
		if (printouts) System.out.println("setHoldability");
		throw new SQLFeatureNotSupportedException("unwrap in SQConnection");
	}
	
	@Override
	public Savepoint setSavepoint() throws SQLException {
		log("inside setSavepoint SQConnection");
		if (printouts) System.out.println("setSavepoint");
		throw new SQLFeatureNotSupportedException("setSavepoint in SQConnection");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		log("inside setSavepoint 2 SQConnection");
		if (printouts) System.out.println("setSavepoint");
		throw new SQLFeatureNotSupportedException("setSavepoint 2 in SQConnection");
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException { 
		log("inside setTransactionIsolation SQConnection");
		if (printouts) System.out.println("setTransactionIsolation");
		throw new SQLFeatureNotSupportedException("setTransactionIsolation in SQConnection");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {	
		log("inside setTypeMap SQConnection");
		if (printouts) System.out.println("setTypeMap");
		throw new SQLFeatureNotSupportedException("setTypeMap in SQConnection");
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		log("inside abort SQConnection");
		if (printouts) System.out.println("abort");
		throw new SQLFeatureNotSupportedException("abort in SQConnection");
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		if (printouts) System.out.println("setSchema");
		throw new SQLFeatureNotSupportedException("setSchema in SQConnection");
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		if (printouts) System.out.println("prepareStatement6");
		throw new SQLFeatureNotSupportedException("prepareStatement in SQConnection");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		if (printouts) System.out.println("releaseSavepoint");
		throw new SQLFeatureNotSupportedException("releaseSavepoint in SQConnection");
	}

	@Override
	public void rollback() throws SQLException {
		if (printouts) System.out.println("rollback");
		throw new SQLFeatureNotSupportedException("rollback in SQConnection");
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		if (printouts) System.out.println("rollback2");
		throw new SQLFeatureNotSupportedException("rollback in SQConnection");
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		// System.out.println("prepareStatement2");
		throw new SQLFeatureNotSupportedException("prepareStatement 2 in SQConnection");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		// System.out.println("prepareStatement3");
		throw new SQLFeatureNotSupportedException("prepareStatement 3 in SQConnection");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		if (printouts) System.out.println("prepareStatement4");
		throw new SQLFeatureNotSupportedException("prepareStatement 4 in SQConnection");
	}
	
	@Override
	public String nativeSQL(String sql) throws SQLException {
		// System.out.println("nativeSQL");
		throw new SQLFeatureNotSupportedException("nativeSQL in SQConnection");
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		// System.out.println("prepareCall");
		throw new SQLFeatureNotSupportedException("prepareCall in SQConnection");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		// System.out.println("prepareCall2");
		throw new SQLFeatureNotSupportedException("prepareCall 2 in SQConnection");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		// System.out.println("prepareCall3");
		throw new SQLFeatureNotSupportedException("prepareCall 3 in SQConnection");
	}
	
	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// System.out.println("getTypeMap");
		throw new SQLFeatureNotSupportedException("getTypeMap in SQConnection");
	}
	
	@Override
	public String getClientInfo(String name) throws SQLException {
//		System.out.println("getClientInfo2");
		throw new SQLFeatureNotSupportedException("getClientInfo in SQConnection");
	}
	
	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
//		System.out.println("createStruct");
		throw new SQLFeatureNotSupportedException("createStruct in SQConnection");
	}
	
	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
//		System.out.println("createArrayOf");
		throw new SQLFeatureNotSupportedException("createArrayOf in SQConnection");
	}

	@Override
	public Blob createBlob() throws SQLException {
//		System.out.println("createBlob");
		throw new SQLFeatureNotSupportedException("createBlob in SQConnection");
	}

	@Override
	public Clob createClob() throws SQLException {
//		System.out.println("createClob");
		throw new SQLFeatureNotSupportedException("createClob in SQConnection");
	}

	@Override
	public NClob createNClob() throws SQLException {
//		System.out.println("createNClob");
		throw new SQLFeatureNotSupportedException("createNClob in SQConnection");
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
//		System.out.println("createSQLXML");
		throw new SQLFeatureNotSupportedException("createSQLXML in SQConnection");
	}
	
}
