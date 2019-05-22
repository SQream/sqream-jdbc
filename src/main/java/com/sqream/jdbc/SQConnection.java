package com.sqream.jdbc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.script.ScriptException;

import com.sqream.jdbc.Connector;
import com.sqream.jdbc.Connector.ConnException;

//Logging
import java.util.Arrays;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;


public class SQConnection implements Connection {
	
	boolean logging = true;
	Path SQConnection_log = Paths.get("./SQConnection.txt");
	boolean log(String line) throws SQLException {
		if (!logging)
			return true;
		
		try {
			Files.write(SQConnection_log, Arrays.asList(new String[] {line}), UTF_8, CREATE, APPEND);
		} catch (IOException e) {
			e.printStackTrace();
			throw new SQLException ("Error writing to SQConnection log");
		}
		
		return true;
	}
	
	class Params {

		Boolean Cluster;
		String LB_ip;
		int LB_port;
		String ip;
		int port;
		String User;
		String Password;
		String DB_name;
		Boolean Use_ssl;
		String service;
	}
	
	 static void print(Object printable) {
	        System.out.println(printable);
    }
	
	private  Vector<SQStatment> Statement_list = new Vector<SQStatment>();

	Connector globalClient = null;
	boolean printouts = false;
	SQDatabaseMetaData data = null;
	Params sqlb= new Params();
    AtomicBoolean IsClosed = new AtomicBoolean(true);
    String username;
    Properties connInfo;
    String db_name;
    String DEFAULT_SERVICE = "sqream";
    
	public SQConnection(Connector client) throws IOException {
		globalClient = client;
	}

	void RemoveItem(Statement obj)
	{
		// System.out.println("RemoveItem");
		if(Statement_list.isEmpty())
			return;
		
		Statement_list.remove(obj);
	}
	
	public SQConnection(Properties connectionInfo) 
			throws ScriptException, SQLException, NumberFormatException, IOException, NoSuchAlgorithmException, KeyManagementException, ConnException {
		
		log("inside constructor SQConnection");
		connInfo = connectionInfo;
		String cluster = connectionInfo.getProperty("cluster");

		String ipaddress = connectionInfo.getProperty("host");
		if (ipaddress.equals("-1"))
			throw new SQLException("missing host error");

		String s_port = connectionInfo.getProperty("port");
		if (s_port.equals("-1"))
			throw new SQLException("missing port error");
			
		db_name = connectionInfo.getProperty("dbname");
		if (db_name.equals("-1"))
			throw new SQLException("missing database name error");

		String service = connectionInfo.getProperty("service");
		if(service == null || service.equals("-1")) {
			System.out.println ("no service passed, defaulting to sqream");
			service = "sqream";
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
		if (skipPicker == null)
			skipPicker = "false";

		boolean isCluster=cluster.equalsIgnoreCase("true");
		
		boolean use_ssl = false;
		String SSL_Connection = connectionInfo.getProperty("ssl");
		if (SSL_Connection == null || SSL_Connection.isEmpty()) {
			use_ssl = true;
		}
		else if (SSL_Connection.equalsIgnoreCase("true")){
			use_ssl = true;
		}
		
		//boolean use_ssl= SSL_Connection != null && !SSL_Connection.isEmpty()? true:false;
		globalClient = new Connector(ipaddress, Integer.parseInt(s_port), cluster.equalsIgnoreCase("true"), use_ssl);
		globalClient.connect(db_name, usr, pswd, service);
		
		sqlb.Cluster=isCluster;
		sqlb.ip=ipaddress;
		sqlb.port=Integer.parseInt(s_port);		
		sqlb.DB_name=db_name;
		sqlb.Password=pswd;
		sqlb.User=usr;
		sqlb.Use_ssl=use_ssl;
		sqlb.service = service;
		IsClosed.set(false);
	}


	@Override
	public Statement createStatement() throws SQLException {
		log("inside constructor SQConnection");
		// System.out.println("createStatement");
		SQStatment SQS;
		try {
			SQS = new SQStatment(globalClient,this, db_name);
			Statement_list.addElement(SQS);
		}catch (Exception e) {
			throw new SQLException(e);
		}

		return SQS;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		log("inside constructor SQConnection");
//		System.out.println("createStatement2");
		String[] lables = { "resultSetType", "resultSetConcurrency" };
		String[] values = { Integer.toString(resultSetType), Integer.toString(resultSetConcurrency) };


		SQStatment SQS = null;
		try {
			SQS = new SQStatment(globalClient,this, db_name);
			Statement_list.addElement(SQS);
		} catch (Exception e) {
			throw new SQLException(e);
		}

		return SQS;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		log("inside createStatement SQConnection");
//		System.out.println("createStatement3");
		String[] lables = { "resultSetType", "resultSetConcurrency", "resultSetHoldability" };
		String[] values = { Integer.toString(resultSetType), Integer.toString(resultSetConcurrency), Integer.toString(resultSetHoldability) };


		SQStatment SQS = null;
		try {
			SQS = new SQStatment(globalClient,this, db_name);
			Statement_list.addElement(SQS);
			
		} catch (Exception e) {
			throw new SQLException(e);
		}

		return SQS;
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		log("inside prepareStatement SQConnection");
		// System.out.println("prepareStatement");
		SQPreparedStatment SQPS = null;
		try {
			SQPS = new SQPreparedStatment(globalClient, sql, this, db_name);
		} catch (KeyManagementException | NoSuchAlgorithmException | IOException | SQLException | ScriptException | ConnException e) {
			throw new SQLException(e);
		} return SQPS;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		log("inside prepareStatement 2 SQConnection");
		if (printouts) System.out.println("prepareStatement5");
		//spark use this function 
		//sql = sql.replace("\"", "");
		SQPreparedStatment SQPS = null;
		try {
			SQPS = new SQPreparedStatment(globalClient, sql, this, db_name);
		} catch (KeyManagementException | NoSuchAlgorithmException | IOException | ScriptException | ConnException e) {
			e.printStackTrace();
			throw new SQLException(e);
		} 
		return SQPS;
		/*
		try {
			SqreamLog.writeInfo("",null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("exit prepareStatement null 4");
		return null;*/
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
			if(globalClient !=null && globalClient.is_open())
				globalClient.close_connection();      // Closing Connector
			IsClosed.set(true);

		} catch (IOException | ScriptException | ConnException e) {
			// TODO Auto-generated catch block
			throw new SQLException(e);
		} 

	}
	
	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		log("inside getMetaData SQConnection");
		//System.out.println("getMetaData");
		if (data == null) {

			try {
				data = new SQDatabaseMetaData(globalClient,this, username, db_name);
				// data = new SQDatabaseMetaData(globalClient);
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
		// System.out.println("isClosed");
		return IsClosed.get();
	}
	
	
	@Override
	public void commit() throws SQLException {
		log("inside commit SQConnection");
		// System.out.println("commit");
	}
	
	@Override
	public boolean getAutoCommit() throws SQLException {
		log("inside getAutoCommit SQConnection");
		// System.out.println("getAutoCommit");
		return true;
	}

	@Override
	public String getCatalog() throws SQLException {
		log("inside getCatalog SQConnection");
		// System.out.println("getCatalog");
		String Catalog = sqlb.DB_name;

		return Catalog;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		log("inside getClientInfo SQConnection");
		// System.out.println("getClientInfo");
		// TODO check functionality
		Properties props = new Properties();
//		props.put("tcpConnectionTime", SqreamJsonHelper.TcpConnectionTime);
//		props.put("sqreamConnectionTime", SqreamJsonHelper.SqreamConnectionTime);
		return props;
	}

	@Override
	public int getHoldability() throws SQLException {
		log("inside getHoldability SQConnection");
//		System.out.println("getHoldability");
		return 0;
	}
	
	@Override
	public int getTransactionIsolation() throws SQLException {
		log("inside getTransactionIsolation SQConnection");
//		System.out.println("getTransactionIsolation");
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
		// System.out.println("isReadOnly");
		return false;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		log("inside isValid SQConnection");
		// System.out.println("isValid");
		return globalClient.is_open();
	}
	
	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		log("inside setAutoCommit SQConnection");
		if (printouts) System.out.println("setAutoCommit");
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
		if (printouts) System.out.println("setNetworkTimeout");
		//throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		log("inside isWrapperFor SQConnection");
//		System.out.println("isWrapperFor");
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		log("inside unwrap SQConnection");
//		System.out.println("unwrap");
		throw new SQLFeatureNotSupportedException("unwrap in SQConnection");

	}

	@Override
	public void clearWarnings() throws SQLException {
		log("inside clearWarnings SQConnection");
//		System.out.println("clearWarnings");
	}
	
	// Unsupported
	// -----------
	
	@Override
	public void setHoldability(int holdability) throws SQLException {
		log("inside setHoldability SQConnection");
		if (printouts) System.out.println("setHoldability");
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public Savepoint setSavepoint() throws SQLException {
		log("inside setSavepoint SQConnection");
		if (printouts) System.out.println("setSavepoint");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		log("inside setSavepoint 2 SQConnection");
		if (printouts) System.out.println("setSavepoint");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException { 
		log("inside setTransactionIsolation SQConnection");
		if (printouts) System.out.println("setTransactionIsolation");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {	
		log("inside setTypeMap SQConnection");
		if (printouts) System.out.println("setTypeMap");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		log("inside abort SQConnection");
		if (printouts) System.out.println("abort");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getSchema() throws SQLException {
		log("inside getSchema SQConnection");
		if (printouts) System.out.println("getSchema");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		if (printouts) System.out.println("setSchema");
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		if (printouts) System.out.println("prepareStatement6");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		if (printouts) System.out.println("releaseSavepoint");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback() throws SQLException {
		if (printouts) System.out.println("rollback");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		if (printouts) System.out.println("rollback2");
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		// System.out.println("prepareStatement2");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		// System.out.println("prepareStatement3");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		if (printouts) System.out.println("prepareStatement4");
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public String nativeSQL(String sql) throws SQLException {
		// System.out.println("nativeSQL");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		// System.out.println("prepareCall");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		// System.out.println("prepareCall2");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		// System.out.println("prepareCall3");
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// System.out.println("getTypeMap");
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public String getClientInfo(String name) throws SQLException {
//		System.out.println("getClientInfo2");
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
//		System.out.println("createStruct");
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
//		System.out.println("createArrayOf");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException {
//		System.out.println("createBlob");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() throws SQLException {
//		System.out.println("createClob");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException {
//		System.out.println("createNClob");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
//		System.out.println("createSQLXML");
		throw new SQLFeatureNotSupportedException();
	}
	
}
