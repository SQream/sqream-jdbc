package com.sqream.jdbc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.script.ScriptException;

import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorFactory;
import com.sqream.jdbc.connector.ConnectorImpl;
import com.sqream.jdbc.connector.ConnectorImpl.ConnException;


public class SQStatment implements Statement {

	private int NO_LIMIT = 0;
	private int SIZE_RESULT = NO_LIMIT; // 0 means no limit.
	private Connector client;
	private SQResultSet resultSet = null;
	private SQConnection connection;
	private int statementId = -1;
	private AtomicBoolean IsCancelStatement = new AtomicBoolean(false);
	private String dbName;
	private boolean isClosed;

	SQStatment(SQConnection conn, String catalog, ConnectorFactory connectorFactory)
			throws NumberFormatException, IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException {
		this.connection = conn;
		this.dbName = catalog;
		this.client = connectorFactory.initConnector(conn.getParams().getIp(), conn.getParams().getPort(),
				conn.getParams().getCluster(), conn.getParams().getUseSsl());
		this.client.connect(conn.getParams().getDbName(), conn.getParams().getUser(), conn.getParams().getPassword(),
				conn.getParams().getService());
		this.isClosed = false;
	}
	
	@Override
	public void cancel() throws SQLException  {

		if (client.checkCancelStatement().getAndSet(true)) {
			return;
		}
		if (!client.isOpenStatement()) {
			return;
		}
		
		statementId = client.getStatementId();
		String sql = "select stop_statement(" + statementId + ")";
		
		ConnectorImpl cancel = null;
		try {
			cancel = new ConnectorImpl(connection.getParams().getIp(), connection.getParams().getPort(), connection.getParams().getCluster(), connection.getParams().getUseSsl());
			cancel.connect(connection.getParams().getDbName(), connection.getParams().getUser(), connection.getParams().getPassword(), connection.getParams().getService());
			cancel.execute(sql);	
			client.setOpenStatement(false);
		} catch (IOException | ConnException | ScriptException | NoSuchAlgorithmException | KeyManagementException e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		} 
		finally  {
			// TODO Auto-generated catch block
			if(cancel !=null && cancel.isOpen())
				try {
					if (cancel.isOpenStatement())
						cancel.close();
					cancel.closeConnection();
				} catch (IOException | ConnException | ScriptException e) {
					// TODO Auto-generated catch block
					String message = e.getMessage();
					if (!message.contains("The statement has already ended")) {
						e.printStackTrace();
						throw new SQLException(e.getMessage());
					}
				}
			IsCancelStatement.set(false);
		}
	}

	@Override
	public void close() throws SQLException {
		try {
			if(client !=null && client.isOpen()) {
				if (client.isOpenStatement())
					client.close();
				client.closeConnection(); // since each statement creates a new client connection.
			}
            if(connection !=null)
			   connection.removeItem(this);
		} catch (IOException | ConnException | ScriptException e) {
			e.printStackTrace();
			throw new SQLException("Statement already closed. Error: " + e);
		} 
	    isClosed = true;
	}	
	
	@Override
	public Connection getConnection() throws SQLException {
		//TODO razi
		SQConnection conn = new SQConnection(client);
		conn.setParams(connection.getParams());
		return conn;
	}
	
	@Override
	public boolean execute(String sql) throws SQLException {
		// Related to bug BG-469 - return true only if this sql should return
		// result
		boolean result = false;

		if (client == null) {
			
		} 
		else

			try {
				statementId = client.execute(sql);
				
				// Omer and Razi - support cancel
				if (IsCancelStatement.get()) {
					//Close the connection of cancel statement
					client.closeConnection();
					throw new SQLException("Statement cancelled by user");
				}

				if ((client.getQueryType() != "INSERT") && client.getRowLength() > 0)
					result = true;

				resultSet = new SQResultSet(client, dbName);
				resultSet.setMaxRows(SIZE_RESULT);
			} catch (Exception e) {
				e.printStackTrace();
				if (e.getMessage().contains("stopped by user")
						|| e.getMessage().contains("cancelled by user")) {
					throw new SQLException("Statement cancelled by user");
				} else {
					throw new SQLException("can not execute - "
							+ e.getMessage());
				}

			}
		return result;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		try {
			statementId = client.execute(sql);

			// BG-1742 hack for workbench data tag , close statement after size
			// of rows
			//if (SIZE_RESULT > 0)
				//stmt.TotalFetch = SIZE_RESULT;
			//SqreamLog.writeInfo(SqreamLog.setLogsParams("sql", sql), Client);
			
			if (IsCancelStatement.get()) {
				//Close the connection of cancel statement
				client.closeConnection();
				throw new SQLException("Statement cancelled by user");
			}

			resultSet = new SQResultSet(client, dbName);
			resultSet.setMaxRows(SIZE_RESULT);
			// Related to bug BG-910 - Set as empty since there is no need to
			// return result
			if ((!"INSERT".equals(client.getQueryType())) && client.getRowLength() == 0)
				resultSet.setEmpty(true);
			
			return resultSet;

		} catch (IOException | ConnException | ScriptException | KeyManagementException | NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			throw new SQLException(e);
		}
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		try {
			statementId = client.execute(sql);
		} catch (IOException | ConnException | ScriptException | KeyManagementException | NoSuchAlgorithmException e) {
			throw new SQLException(e);
		} 
		return 0;
	}
	
	@Override
	public void setFetchSize(int arg0) throws SQLException {
		SIZE_RESULT = arg0;
	}

	@Override
	public void setMaxRows(int max_rows) throws SQLException {
		try {
			client.setFetchLimit(max_rows);
		} catch (ConnException e) {
			throw new SQLException("Error in setMaxRows:" + e);
		}
	}
	
	@Override
	public int getMaxRows() throws SQLException {
		return client.getFetchLimit();
	}

	/*
	 Moves to this Statement object's next result, returns true if it is a ResultSet 
	 object, and implicitly closes any current ResultSet object(s) obtained with the 
	 method getResultSet.

	 There are no more results when the following is true:

    // stmt is a Statement object
    ((stmt.getMoreResults() == false) && (stmt.getUpdateCount() == -1))

	Returns:
   	true if the next result is a ResultSet object; false if it is an update count or there are no more results
	 */
	@Override
	public boolean getMoreResults() throws SQLException {
		if (this.isClosed) { // Spec: if a database access error occurs or this method is called on a closed Statement
			throw new SQLException("Called getMoreResults() on closed statement");
		}
		return false;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		//if a database access error occurs, this method is called on a closed Statement or the argument supplied is
		// not one of the following:
		// Statement.CLOSE_CURRENT_RESULT, Statement.KEEP_CURRENT_RESULT or Statement.CLOSE_ALL_RESULTS
		return false;
	}
	
	@Override
	public ResultSet getResultSet() throws SQLException {

		return resultSet;
	}
	
	@Override
	public int getUpdateCount() throws SQLException {
		return -1;
		// throw new SQLFeatureNotSupportedException();  // -1
	}

	/*
	 Retrieves the first warning reported by calls on this Statement object. Subsequent
	  Statement object warnings will be chained to this SQLWarning object.

	The warning chain is automatically cleared each time a statement is (re)executed.
	 This method may not be called on a closed Statement object; doing so will cause an 
	 SQLException to be thrown.

	Note: If you are processing a ResultSet object, any warnings associated with reads
	 on that ResultSet object will be chained on it rather than on the Statement object
	  that produced it.

	Returns:
	    the first SQLWarning object or null if there are no warnings
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	//<editor-fold desc="Unsupported">
	// Unsupported
	// -----------
	
	@Override
	public void clearBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException("clearBatch in SQStatement");
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException("clearWarnings in SQStatement");
	}
	
	@Override
	public void setQueryTimeout(int arg0) throws SQLException {
		if (arg0 !=0)  // 0 means unlimited timeout
			throw new SQLFeatureNotSupportedException("setQueryTimeout in SQStatement");
	}
	
	@Override
	public int executeUpdate(String arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("executeUpdate in SQStatement");
	}

	@Override
	public int executeUpdate(String arg0, int[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("executeUpdate 2 in SQStatement");
	}

	@Override
	public int executeUpdate(String arg0, String[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("executeUpdate 3 in SQStatement");
	}

	@Override
	public boolean execute(String arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("execute in SQStatement");
	}

	@Override
	public boolean execute(String arg0, int[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("execute 2 in SQStatement");
	}

	@Override
	public boolean execute(String arg0, String[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("execute 3 in SQStatement");
	}

	@Override
	public int[] executeBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException("executeBatch in SQStatement");
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("unwrap in SQStatement");
	}

	@Override
	public void addBatch(String arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException("addBatch in SQStatement");
	}
	
	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLFeatureNotSupportedException("getFetchDirection in SQStatement");
	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new SQLFeatureNotSupportedException("getFetchSize in SQStatement");
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException("getGeneratedKeys in SQStatement");
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		throw new SQLFeatureNotSupportedException("getMaxFieldSize in SQStatement");
	}
	
	@Override
	public void setEscapeProcessing(boolean arg0) throws SQLException {
		/*
		Sets escape processing on or off. If escape scanning is on (the default), 
		the driver will do escape substitution before sending the SQL statement to 
		the database. Note: Since prepared statements have usually been parsed prior 
		to making this call, disabling escape processing for PreparedStatements objects 
		will have no effect.
		*/
	}
	
	@Override
	public int getQueryTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException("getQueryTimeout in SQStatement");
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException("getResultSetConcurrency in SQStatement");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("getResultSetHoldability in SQStatement");
	}

	@Override
	public int getResultSetType() throws SQLException {
		throw new SQLFeatureNotSupportedException("getResultSetType in SQStatement");
	}
	/* Retrieves the current result as an update count; if the result is a ResultSet 
	* object or there are no more results, -1 is returned. This method should be 
	* called only once per result.(non-Javadoc)
	*/

	@Override
	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException("isPoolable in SQStatement");
	}

	@Override
	public void setCursorName(String arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException("setCursorName in SQStatement");
	}

	@Override
	public void setFetchDirection(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException("setFetchDirection in SQStatement");
	}

	@Override
	public void setMaxFieldSize(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException("setMaxFieldSize in SQStatement");
	}
	
	@Override
	public void setPoolable(boolean arg0) throws SQLException {
            throw new SQLFeatureNotSupportedException("setPoolable in SQStatement");
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException("closeOnCompletion in SQStatement");
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException("isCloseOnCompletion in SQStatement");
	}
	//</editor-fold>
}
