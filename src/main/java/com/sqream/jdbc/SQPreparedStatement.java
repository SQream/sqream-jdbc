package com.sqream.jdbc;


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

import java.util.ArrayList;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;


import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorImpl;
import com.sqream.jdbc.connector.ConnException;


public class SQPreparedStatement implements PreparedStatement {
    private static final Logger LOGGER = Logger.getLogger(SQPreparedStatement.class.getName());

    private Connector client;
    private SQResultSet SQRS = null;
    private SQResultSetMetaData metaData = null;
    private int statement_id;
    private String db_name;
    private int setCounter = 0;
    private List<Integer> setsPerBatch = new ArrayList<>();
    private boolean isClosed = true;

    public SQPreparedStatement(String sql, ConnectionParams connParams) throws ConnException {

        LOGGER.log(Level.FINE, MessageFormat.format("Construct SQPreparedStatement for [{0}]", sql));
        db_name = connParams.getDbName();
        isClosed = false;
        client = new ConnectorImpl(connParams.getIp(), connParams.getPort(), connParams.getCluster(), connParams.getUseSsl());
        client.connect(connParams.getDbName(), connParams.getUser(), connParams.getPassword(), "sqream");  // default service
        if (connParams.getFetchSize() != null && connParams.getFetchSize() > 0) {
            client.setFetchSize(connParams.getFetchSize());
        }
        statement_id = client.execute(sql);
        metaData = new SQResultSetMetaData(client, connParams.getDbName());
    }

    
    @Override
    public void close() throws SQLException {
    	LOGGER.log(Level.FINE,"Close prepared statement");
        try {
        	if (client != null && client.isOpen()) {
				if (client.isOpenStatement()) {
					client.close();
				}
				client.closeConnection();
        	}
        } catch (Exception e) {
            throw new SQLException(e);
        } 
        isClosed = true;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        LOGGER.log(Level.FINE,"execute batch");

        int[] res = new int[setsPerBatch.size()];
        Arrays.fill(res, 1);
        setsPerBatch.clear();

        return res;
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        return SQRS;
    }

    @Override
    public void addBatch() throws SQLException {
        LOGGER.log(Level.FINEST, "add batch");

        try {
            client.next();
            // Remember how many set commands were issued for this row in case it comes handy
            setsPerBatch.add(setCounter);  
            setCounter = 0;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean execute() {
        LOGGER.log(Level.FINE,"execute");

        SQRS = new SQResultSet(client, db_name);

        //TODO: Duplicate logic in SQStatement
        return (!"INSERT".equals(client.getQueryType())) && client.getRowLength() > 0;
    }
    
    @Override
    public ResultSet executeQuery() {
        LOGGER.log(Level.FINE,"execute query");

        SQRS = new SQResultSet(client, db_name);
        return SQRS;
    }

    // set()
    // ----
    
    @Override
    public void setBoolean(int arg0, boolean arg1) throws SQLException {
        
        try {
			client.setBoolean(arg0, arg1);
		} catch (Exception e) {
            throw new SQLException(e);
		} setCounter++;      
    }

    @Override
    public void setByte(int arg0, byte arg1) throws SQLException {
        
    	try {
			client.setUbyte(arg0, arg1);
		} catch (Exception e) {
            throw new SQLException(e);
		} setCounter++; 
    }
    
    @Override
    public void setShort(int arg0, short arg1) throws SQLException {
	    try {
			client.setShort(arg0, arg1);
		} catch (Exception e) {
            throw new SQLException(e);
		} setCounter++;    
    }

    @Override
    public void setInt(int arg0, int arg1) throws SQLException {

    	try {
			client.setInt(arg0, arg1);
		} catch (Exception e) {
            throw new SQLException(e);
		} setCounter++;    
    }

    @Override
    public void setLong(int arg0, long arg1) throws SQLException {
        
        try {
			client.setLong(arg0, arg1);
		} catch (Exception e) {
            throw new SQLException(e);
		} setCounter++; 
    }
    
    @Override
    public void setFloat(int arg0, float arg1) throws SQLException {
        try {
			client.setFloat(arg0, arg1);
		} catch (Exception e) {
            throw new SQLException(e);
		} setCounter++;    
    }
    
    @Override
    public void setDouble(int arg0, double arg1) throws SQLException {
        
         try {
			client.setDouble(arg0, arg1);
		} catch (Exception e) {
             throw new SQLException(e);
		} setCounter++;  
    }
    
    @Override
    public void setDate(int colNum, Date date) throws SQLException {
        try {
			client.setDate(colNum, date);
		} catch (Exception e) {
            throw new SQLException(e.getMessage());
		} setCounter++; 
    }

    @Override
    public void setDate(int colNum, Date date, Calendar cal) throws SQLException {
    	
    	try {
			client.setDate(colNum, date, cal.getTimeZone().toZoneId());
		} catch (Exception e) {
            throw new SQLException(e);
		} setCounter++; 
    }
    
    @Override
    public void setTimestamp(int colNum, Timestamp datetime) throws SQLException {
        
    	try {
			client.setDatetime(colNum, datetime);
		} catch (Exception e) {
            throw new SQLException(e);
		} setCounter++; 
    }

    @Override
    public void setTimestamp(int colNum, Timestamp datetime, Calendar cal) throws SQLException {
    	try {
			client.setDatetime(colNum, datetime, cal.getTimeZone().toZoneId());
		} catch (Exception e) {
            throw new SQLException(e);
		}
    	setCounter++;
    }
    
    @Override
    public void setString(int colNum, String value) throws SQLException {
        String type = getMetaData().getColumnTypeName(colNum);
        try {
            if ("Varchar".equals(type)) {
                client.setVarchar(colNum, value);
            } else if ("NVarchar".equals(type)) {
                client.setNvarchar(colNum, value);
            } else {
                throw new IllegalArgumentException(
                        MessageFormat.format("Trying to set string on a column number [{0}] of type [{1}]",
                                colNum, type));
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
        setCounter++;   
    }

    @Override
    public void setNString(int arg0, String arg1) throws SQLException {
        try {
			client.setNvarchar(arg0, arg1);
		} catch (Exception e) {
            throw new SQLException(e);
		} setCounter++; 
    }
    
    @Override
    public void setNull(int arg0, int arg1) throws SQLException {
        
    	String type = "";
    	
    	try {
    	    type = client.getColType(arg0);

            switch (type) {
                case "ftBool":
                    client.setBoolean(arg0, null);
                    break;
                case "ftUByte":
                    client.setUbyte(arg0, null);
                    break;
                case "ftShort":
                    client.setShort(arg0, null);
                    break;
                case "ftInt":
                    client.setInt(arg0, null);
                    break;
                case "ftLong":
                    client.setLong(arg0, null);
                    break;
                case "ftFloat":
                    client.setFloat(arg0, null);
                    break;
                case "ftDouble":
                    client.setDouble(arg0, null);
                    break;
                case "ftDate":
                    client.setDate(arg0, null);
                    break;
                case "ftDateTime":
                    client.setDatetime(arg0, null);
                    break;
                case "ftVarchar":
                    client.setVarchar(arg0, null);
                    break;
                case "ftBlob":
                    client.setNvarchar(arg0, null);
                    break;
                default:
                    throw new IllegalArgumentException(
                            MessageFormat.format("Unsupported column type [{0}]", type));
            }
    	    setCounter++;   
        } catch (Exception e) {
            throw new SQLException(e);
		} 
    }

    @Override
    public void setObject(int colNum, Object value) throws SQLException {

        if (value instanceof Boolean) {
            setBoolean(colNum, (Boolean) value);
        } else if (value instanceof Byte) {
            setByte(colNum, (Byte) value);
        } else if (value instanceof Short) {
            setShort(colNum, (Short) value);
        } else if (value instanceof Integer) {
            setInt(colNum, (Integer) value);
        } else if (value instanceof Long) {
            setLong(colNum, (Long) value);
        } else if (value instanceof Float) {
            setFloat(colNum, (Float) value);
        } else if (value instanceof Double) {
            setDouble(colNum, (Double) value);
        }  else if (value instanceof Date) {
            setDate(colNum, (Date) value);
        } else if (value instanceof Timestamp) {
            setTimestamp(colNum, (Timestamp) value);
        } else if (value instanceof String) {
            setString(colNum, (String) value);
        } else 
            throw new SQLException(MessageFormat.format(
                    "Type [{0}] for setObject not supported", value != null ? value.getClass().getName() : null));
    }
    
    
    
    public ResultSetMetaData getMetaData() throws SQLException {
        
        if (metaData == null)
            throw new SQLException("MetaData is empty");
        return metaData;
    }

    // ----------------
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
       if (rows <0)
           throw new SQLException("setFetchSize argument should be nonnegative, got " + rows);
    }
    
    @Override
    public boolean execute(String arg0) throws SQLException {
        throw new SQLException("Method is called on a PreparedStatement");
    }

    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        throw new SQLException("Method is called on a PreparedStatement");
    }

    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        throw new SQLException("Method is called on a PreparedStatement");
    }

    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        throw new SQLException("Method is called on a PreparedStatement");
    }
    
    @Override
    public int executeUpdate(String arg0) throws SQLException {
        throw new SQLException("Method is called on a PreparedStatement");
    }

    @Override
    public int executeUpdate(String arg0, int arg1) throws SQLException {
        throw new SQLException("Method is called on a PreparedStatement");
    }

    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        throw new SQLException("Method is called on a PreparedStatement");
    }

    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        throw new SQLException("Method is called on a PreparedStatement");
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return client.getFetchSize();
    }
    
    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return client.getFetchLimit();
    }

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
        if (this.isClosed) {
            throw new SQLException("Called getQueryTimeout() on closed statement");
        }
        return this.client.getTimeout();
    }
    
    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }
    
    @Override
    public boolean isClosed() throws SQLException {
    	return isClosed;
    }
    
    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }
    
    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }	
    
    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public void clearWarnings() throws SQLException {

    }
    
    @Override
    public void setNull(int arg0, int arg1, String arg2) throws SQLException {
        
    }
    
    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return false;
    }
    
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new SQParameterMetaData(client);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (this.isClosed) {
            throw new SQLException("Called setQueryTimeout() on closed statement");
        }
        if (seconds < 0) {
            throw new SQLException(MessageFormat.format("Query timeout [{0}] must be a value greater than or equals to 0.", seconds));
        }
        this.client.setTimeout(seconds);
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        try {
            client.setFetchLimit(maxRows);
        } catch (Exception e) {
            throw new SQLException("Error in setMaxRows:" + e);
        }
    }

    // Unsupported

    // -----------

    @Override   
    public void addBatch(String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException("addBatch in SQPreparedStatement");
    }
    
    @Override
    public ResultSet executeQuery(String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException("executeQuery in SQPreparedStatement");
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        throw new SQLFeatureNotSupportedException("getConnection in SQPreparedStatement");
    }
    
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("getGeneratedKeys in SQPreparedStatement");
    }
     
    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("getWarnings in SQPreparedStatement");
    }

    @Override
    public void setCursorName(String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCursorName in SQPreparedStatement");
    }

    @Override
    public void setEscapeProcessing(boolean arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setEscapeProcessing in SQPreparedStatement");
    }

    @Override
    public void setFetchDirection(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setFetchDirection in SQPreparedStatement");
    }

    @Override
    public void setMaxFieldSize(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setMaxFieldSize in SQPreparedStatement");
    }

    @Override
    public void setPoolable(boolean arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setPoolable in SQPreparedStatement");
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException("unwrap in SQPreparedStatement");
    }
    
    @Override
    public void clearParameters() throws SQLException {
        throw new SQLFeatureNotSupportedException("clearParameters in SQPreparedStatement");
    }
    
    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("executeUpdate in SQPreparedStatement");
    }
    
    @Override
    public void setArray(int arg0, Array arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray in SQPreparedStatement");
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream in SQPreparedStatement");
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream 2 in SQPreparedStatement");
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream 3 in SQPreparedStatement");
    }

    @Override
    public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBigDecimal in SQPreparedStatement");
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream in SQPreparedStatement");
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream 2 in SQPreparedStatement");
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream 3 in SQPreparedStatement");
    }

    @Override
    public void setBlob(int arg0, Blob arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob in SQPreparedStatement");
    }

    @Override
    public void setBlob(int arg0, InputStream arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob 2 in SQPreparedStatement");
    }

    @Override
    public void setBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob 3 in SQPreparedStatement");
    }
    
    @Override
    public void setBytes(int arg0, byte[] arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBytes in SQPreparedStatement");
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream in SQPreparedStatement");
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream 2 in SQPreparedStatement");
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream 3 in SQPreparedStatement");
    }

    @Override
    public void setClob(int arg0, Clob arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob in SQPreparedStatement");
    }

    @Override
    public void setClob(int arg0, Reader arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob 2 in SQPreparedStatement");
    }

    @Override
    public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob 3 in SQPreparedStatement");
    }
    
    @Override
    public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream in SQPreparedStatement");
    }

    @Override
    public void setNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream 2 in SQPreparedStatement");
    }

    @Override
    public void setNClob(int arg0, NClob arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob in SQPreparedStatement");
    }

    @Override
    public void setNClob(int arg0, Reader arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob in SQPreparedStatement");
    }

    @Override
    public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob in SQPreparedStatement");
    }
    
    @Override
    public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setObject in SQPreparedStatement");
    }

    @Override
    public void setObject(int arg0, Object arg1, int arg2, int arg3) throws SQLException {
        throw new SQLFeatureNotSupportedException("setObject 2 in SQPreparedStatement");
    }

    @Override
    public void setRef(int arg0, Ref arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef in SQPreparedStatement");
    }

    @Override
    public void setRowId(int arg0, RowId arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId in SQPreparedStatement");
    }

    @Override
    public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSQLXML in SQPreparedStatement");
    }
    
    @Override
    public void setTime(int arg0, Time arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTime in SQPreparedStatement");
    }

    @Override
    public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTime 2 in SQPreparedStatement");
    }

    @Override
    public void setURL(int arg0, URL arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("setURL in SQPreparedStatement");
    }

    @Override
    public void setUnicodeStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream in SQPreparedStatement");
    }
    
    /*
     * public QueryType[] getResType() { return QType; } public void
     * setResType(QueryType[] QType) { this.QType = QType; }
     */
    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException("closeOnCompletion in SQPreparedStatement");
    }

    

}
