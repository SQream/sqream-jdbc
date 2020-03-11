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
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.ConnectorFactory;

import java.util.logging.Level;
import java.util.logging.Logger;


public class SQConnection implements Connection {
	private static final Logger LOGGER = Logger.getLogger(SQConnection.class.getName());

	private static final int[] RESULTSET_TYPES =
			new int[]{ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};
	private static final int[] RESULTSET_CONCURRENCY =
			new int[]{ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};
	private static final int[] RESULTSET_HOLDABILITY =
			new int[]{ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT};

	private Vector<SQStatement> Statement_list = new Vector<>();
	private Connector globalClient;
	private SQDatabaseMetaData data = null;
	private ConnectionParams params;
	private AtomicBoolean isClosed = new AtomicBoolean(true);
    
	SQConnection(Connector client) {
		globalClient = client;
	}
	
	SQConnection(Properties connectionInfo)
			throws ScriptException, SQLException, NumberFormatException, IOException, NoSuchAlgorithmException, KeyManagementException, ConnException {
		if (Level.FINE == LOGGER.getParent().getLevel()) {
			LOGGER.log(Level.FINE,"Construct SQConnection with properties");
			logProperties(connectionInfo);
		}

		this.params = ConnectionParams.builder()
				.cluster(connectionInfo.getProperty("cluster"))
				.ipAddress(connectionInfo.getProperty("host"))
				.port(connectionInfo.getProperty("port"))
				.dbName(connectionInfo.getProperty("dbName"))
				.service(connectionInfo.getProperty("service"))
				.schema(connectionInfo.getProperty("schema"))
				.user(connectionInfo.getProperty("user"))
				.password(connectionInfo.getProperty("password"))
				.useSsl(connectionInfo.getProperty("ssl"))
				.build();

		globalClient = ConnectorFactory.initConnector(
				params.getIp(), params.getPort(), params.getCluster(), params.getUseSsl());

		globalClient.connect(params.getDbName(), params.getUser(), params.getPassword(), params.getService());

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
		LOGGER.log(Level.FINE, "createStatement without params");

		if (isClosed.get()) {
			throw new SQLException(MessageFormat.format("Statement is closed: isClosed=[{0}]", isClosed));
		}

		SQStatement SQS;
		try {
			SQS = new SQStatement(this, params.getDbName());
			Statement_list.addElement(SQS);
		} catch (Exception e) {
			throw new SQLException(e);
		}
		return SQS;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"createStatement with params: resultSetType=[{0}], resultSetConcurrency=[{1}]",
				resultSetType, resultSetConcurrency));

		if (isClosed.get() || !validParams(resultSetType, resultSetConcurrency)) {
			throw new SQLException(MessageFormat.format("Wrong params in createStatement: " +
							"resultSetType=[{0}], resultSetConcurrency=[{1}]",
					resultSetType, resultSetConcurrency));
		}

		SQStatement SQS = null;
		try {
			SQS = new SQStatement(this, params.getDbName());
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
		LOGGER.log(Level.FINE, MessageFormat.format(
				"createStatement with params: resultSetType=[{0}], resultSetConcurrency=[{1}], resultSetHoldability=[{2}]",
				resultSetType, resultSetConcurrency, resultSetHoldability));

		if (isClosed.get() || !validParams(resultSetType, resultSetConcurrency, resultSetHoldability)) {
			throw new SQLException(MessageFormat.format("Wrong params in createStatement: " +
							"resultSetType=[{0}], resultSetConcurrency=[{1}], resultSetHoldability=[{2}]",
					resultSetType, resultSetConcurrency, resultSetHoldability));
		}

		SQStatement SQS;
		try {
			SQS = new SQStatement(this, params.getDbName());
			Statement_list.addElement(SQS);
			
		} catch (Exception e) {
			throw new SQLException(e);
		}

		return SQS;
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("prepareStatement for sql=[{0}]", sql));

		if (isClosed.get()) {
			throw new SQLException(MessageFormat.format("Statement is closed: isClosed=[{0}]", isClosed));
		}

		SQPreparedStatement SQPS = null;
		try {
			SQPS = new SQPreparedStatement(sql, params);
		} catch (Exception e) {
			throw new SQLException(e);
		} return SQPS;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format(
				"prepareStatement with params: sql=[{0}], resultSetType=[{1}], resultSetConcurrency=[{2}]",
				sql, resultSetType, resultSetConcurrency));

		if (isClosed.get()) {
			throw new SQLException(MessageFormat.format("Statement is closed: isClosed=[{0}]", isClosed));
		}

		//spark use this function 
		//sql = sql.replace("\"", "");
		SQPreparedStatement SQPS = null;
		try {
			SQPS = new SQPreparedStatement(sql, params);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException(e);
		} 
		return SQPS;
	}
	
	@Override
	public void close() throws SQLException {
		LOGGER.log(Level.FINE, "Close SQConnection");

		try {
			if(Statement_list!=null) {
				for(SQStatement item : Statement_list) {
					item.cancel();
				}
				Statement_list.clear();
			}
			if(globalClient !=null && globalClient.isOpen())
				globalClient.closeConnection();      // Closing Connector
			isClosed.set(true);

		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
	
	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		LOGGER.log(Level.FINE, "get DatabaseMetaData");

		if (data == null) {
			try {
				data = new SQDatabaseMetaData(globalClient,this, params.getUser(), params.getDbName());
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}
		return data;
	}
	
	@Override
	public boolean isClosed() throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("isClosed returns [{0}]", isClosed.get()));
		return isClosed.get();
	}
	
	@Override
	public void commit() throws SQLException {
		LOGGER.log(Level.FINE, "commit");
	}
	
	@Override
	public boolean getAutoCommit() throws SQLException {
		LOGGER.log(Level.FINE, "getAutoCommit");
		return true;
	}
	
	@Override
	public String getSchema() {
		LOGGER.log(Level.FINE, MessageFormat.format("Schema=[{0}]", params.getSchema()));
		return params.getSchema();
	}
	
	@Override
	public String getCatalog() throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("DBName = [{0}]", params.getDbName()));
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
		LOGGER.log(Level.FINE, MessageFormat.format(
				"Params: timeout=[{0}]. Return [{1}]", timeout, globalClient.isOpen()));
		return globalClient.isOpen();
	}
	
	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		log("inside setAutoCommit SQConnection");
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		log("inside setCatalog SQConnection");
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		log("inside setClientInfo SQConnection");
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		log("inside setClientInfo SQConnection");
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		/* Puts this connection in read-only mode as a hint 
		 * to the driver to enable database optimizations.  */
		log("inside setReadOnly SQConnection");
	}
	
	@Override
	public int getNetworkTimeout() throws SQLException {
		log("inside getNetworkTimeout SQConnection");
		return 0;
	}
	
	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		log("inside setNetworkTimeout SQConnection");
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

	private void log(String line) {
		LOGGER.log(Level.FINE, line);
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
		throw new SQLFeatureNotSupportedException("unwrap in SQConnection");
	}
	
	@Override
	public Savepoint setSavepoint() throws SQLException {
		log("inside setSavepoint SQConnection");
		throw new SQLFeatureNotSupportedException("setSavepoint in SQConnection");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		log("inside setSavepoint 2 SQConnection");
		throw new SQLFeatureNotSupportedException("setSavepoint 2 in SQConnection");
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException { 
		log("inside setTransactionIsolation SQConnection");
		throw new SQLFeatureNotSupportedException("setTransactionIsolation in SQConnection");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {	
		log("inside setTypeMap SQConnection");
		throw new SQLFeatureNotSupportedException("setTypeMap in SQConnection");
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		log("inside abort SQConnection");
		throw new SQLFeatureNotSupportedException("abort in SQConnection");
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw new SQLFeatureNotSupportedException("setSchema in SQConnection");
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareStatement in SQConnection");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException("releaseSavepoint in SQConnection");
	}

	@Override
	public void rollback() throws SQLException {
		throw new SQLFeatureNotSupportedException("rollback in SQConnection");
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException("rollback in SQConnection");
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareStatement 2 in SQConnection");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareStatement 3 in SQConnection");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareStatement 4 in SQConnection");
	}
	
	@Override
	public String nativeSQL(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException("nativeSQL in SQConnection");
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareCall in SQConnection");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareCall 2 in SQConnection");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareCall 3 in SQConnection");
	}
	
	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException("getTypeMap in SQConnection");
	}
	
	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException("getClientInfo in SQConnection");
	}
	
	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException("createStruct in SQConnection");
	}
	
	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new SQLFeatureNotSupportedException("createArrayOf in SQConnection");
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException("createBlob in SQConnection");
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException("createClob in SQConnection");
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException("createNClob in SQConnection");
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException("createSQLXML in SQConnection");
	}

	private void logProperties(Properties props) {
		if (props == null) {
			LOGGER.log(Level.FINE, "Properties is null");
		} else {
			for (String key : props.stringPropertyNames()) {
				LOGGER.log(Level.FINE, MessageFormat.format("[{0}]=[{1}]", key, props.getProperty(key)));
			}
		}
	}
}
