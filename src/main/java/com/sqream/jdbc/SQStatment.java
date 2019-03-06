package com.sqream.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sqream.connector.ConnectionHandle;
import com.sqream.connector.StatementHandle;
import com.sqream.connector.ColumnMetadata;

public class SQStatment implements Statement {

	int NO_LIMIT = 0;
	int SIZE_RESULT = NO_LIMIT; // 0 means no limit.
	private ConnectionHandle Client = null;
    private StatementHandle stmt =null; 
	SQResultSet SQRS = null;
	String SQLCommand = ""; // this string is only for debugging purpose.
	boolean SpecialStatement = false;
	SQConnection Connection=null;
	int statement_id = -1;
    AtomicBoolean IsCancelStatement = new AtomicBoolean(false);
    enum statementType {DML, INSERT, SELECT};
    String db_name;
    
    	
	public SQStatment(ConnectionHandle client,SQConnection conn, String catalog) throws NumberFormatException,
			UnknownHostException, IOException, 
			SQLException, KeyManagementException, NoSuchAlgorithmException {
		Tuple<String ,Integer> params =null;
		Connection=conn;
		db_name = catalog;

		// Related to bug #541 - skip the picker if we are in cancel state
		if (conn.sqlb.Cluster)
		{
		  params= Utils.getLBConnection(conn.sqlb.LB_ip,conn.sqlb.LB_port);
		  conn.sqlb.ip=params.host;
		  conn.sqlb.port=params.port;
		}
		
		Client = new ConnectionHandle(conn.sqlb.ip, conn.sqlb.port,conn.sqlb.User , conn.sqlb.Password, conn.sqlb.DB_name, conn.sqlb.Use_ssl);
		Client.connect();

	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void addBatch(String arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	

	@Override
	public void cancel() throws SQLException  {

		
		if (IsCancelStatement.get())
			return;

		String sql = "select stop_statement(" + statement_id + ")";
		IsCancelStatement.set(true);
		ConnectionHandle cancel=null;
		StatementHandle cancel_stmt=null;
					
		try {
			cancel = new ConnectionHandle(Connection.sqlb.ip, Connection.sqlb.port, Connection.sqlb.User , Connection.sqlb.Password, Connection.sqlb.DB_name, Connection.sqlb.Use_ssl);
			cancel.connect();
			cancel_stmt = new StatementHandle (cancel, sql);
			cancel_stmt.prepare();
			cancel_stmt.execute();			
		}catch (SQLException e) {
			String message = e.getMessage();
			if (!message.contains("The statement has already ended"))
				throw new SQLException(e.getMessage());
			
		}catch (KeyManagementException | NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			throw new SQLException(e.getMessage());
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			throw new SQLException(e.getMessage());
		}
		finally  {
			// TODO Auto-generated catch block
			if(cancel_stmt !=null)
				try {
					if(cancel_stmt !=null)
						cancel_stmt.close();
					if(cancel !=null)
						cancel.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					String message = e.getMessage();
					if (!message.contains("The statement has already ended")) {
						e.printStackTrace();
						throw new SQLException(e.getMessage());
					}

				}
			
			
		}
		
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void close() throws SQLException {
		try {
			if(stmt !=null)
				stmt.close();
			if (Client !=null)
				Client.close(); // since each statement creates a new client connection.
            if(Connection !=null)
			   Connection.RemoveItem(this); 
		} catch (IOException e) {
			// TODO Auto-generated catch block

			throw new SQLException("Statement already closed");
		} 
		
//		catch (NullPointerException e) {}
		// TODO Auto-generated catch block

		
	}	

	@Override
	public boolean execute(String sql) throws SQLException {
		// TODO Auto-generated method stub
		SQLCommand = sql; // so i can debug it later.
		// Related to bug BG-469 - return true only if this sql should return
		// result
		boolean result = false;

		if (Client == null) {
			
		} 
		else

			try {
				stmt = new StatementHandle (Client, sql);
				statement_id =stmt.getStatementId();
				stmt.prepare();
				
				// Omer and Razi - support cancel
				if (IsCancelStatement.get()) {
					//Close the connection of cancel statement
					Client.close();
					throw new SQLException("Statement cancelled by user");
				}

				stmt.execute();

				if ((stmt.getStatementType() != StatementHandle.statementType.INSERT) && stmt.getMetadata().length > 0)
					result = true;

				SQRS = new SQResultSet(stmt, db_name);
				SQRS.MaxRows = SIZE_RESULT;
			} catch (Exception e) {
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
	public boolean execute(String arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String arg0, int[] arg1) throws SQLException {
		// TODO Auto-generated method stub

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String arg0, String[] arg1) throws SQLException {
		// TODO Auto-generated method stub

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		// TODO Auto-generated method stub

		throw new SQLFeatureNotSupportedException();
	}

	private void execute_set(String sql) throws SQLException, KeyManagementException, NoSuchAlgorithmException {
		try {
			stmt = new StatementHandle(Client, sql);
			stmt.prepare();
			// SqrmRespPrepareSql ps=
			// Client.prepareSql(sql,SIZE_RESULT,SpecialStatement);
			 
			// statement_id=ps.statement_id;
			// Omer and Razi - support cancel
			if (IsCancelStatement.get()) {
				//Close the connection of cancel statement
				Client.close();
				throw new SQLException("Statement cancelled by user");
			}
			stmt.execute();

			stmt.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new SQLException(e);
		} 
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		try {

            			
			stmt = new StatementHandle(Client, sql);
			
			statement_id =stmt.getStatementId();
			stmt.prepare();
			
			
			// BG-1742 hack for workbench data tag , close statement after size
			// of rows
			//if (SIZE_RESULT > 0)
				//stmt.TotalFetch = SIZE_RESULT;
			//SqreamLog.writeInfo(SqreamLog.setLogsParams("sql", sql), Client);
			
			if (IsCancelStatement.get()) {
				//Close the connection of cancel statement
				Client.close();
				throw new SQLException("Statement cancelled by user");
			}

			

			try {
				stmt.execute();
			} catch (KeyManagementException | NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			SQRS = new SQResultSet(stmt, db_name);
			SQRS.MaxRows = SIZE_RESULT;
			// Related to bug BG-910 - Set as empty since there is no need to
			// return result
			if ((stmt.getStatementType() != StatementHandle.statementType.INSERT) && stmt.getMetadata().length == 0)
				SQRS.Empty = true;
			

			return SQRS;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new SQLException(e);
		}

	}

	
	@Override
	public int executeUpdate(String sql) throws SQLException {

		try {
			stmt = new StatementHandle(Client, sql);
			statement_id =stmt.getStatementId();
			stmt.prepare();
			try {
				stmt.execute();
			} catch (KeyManagementException | NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (IOException e) {			
			throw new SQLException(e);
		} 
		return 0;
	}

	@Override
	public int executeUpdate(String arg0, int arg1) throws SQLException {

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String arg0, int[] arg1) throws SQLException {

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String arg0, String[] arg1) throws SQLException {

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Connection getConnection() throws SQLException {
		
		try {
			//TODO razi
			SQConnection conn=new SQConnection(Client);
			conn.sqlb=Connection.sqlb;
			return conn;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		// TODO Auto-generated method stub

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxRows() throws SQLException {

		throw new SQLFeatureNotSupportedException();
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

		return false;
	}

	@Override
	public boolean getMoreResults(int arg0) throws SQLException {
	return false;
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {

		return SQRS;
	}


	@Override
	public int getResultSetConcurrency() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetType() throws SQLException {

		throw new SQLFeatureNotSupportedException();
	}
	/* Retrieves the current result as an update count; if the result is a ResultSet 
	* object or there are no more results, -1 is returned. This method should be 
	* called only once per result.(non-Javadoc)
	*/
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
		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isPoolable() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCursorName(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
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
	public void setFetchDirection(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchSize(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		SIZE_RESULT = arg0;

	}

	@Override
	public void setMaxFieldSize(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setMaxRows(int arg0) throws SQLException {
		
		if (arg0 != 0) // if zero, use default
		{
			SIZE_RESULT = arg0;
			if (SQRS != null)
				SQRS.MaxRows = SIZE_RESULT;
		}

	}

	@Override
	public void setPoolable(boolean arg0) throws SQLException {
            throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setQueryTimeout(int arg0) throws SQLException {
		if (arg0 !=0)  // 0 means unlimited timeout
			throw new SQLFeatureNotSupportedException();

	}

	@Override
	public void closeOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
