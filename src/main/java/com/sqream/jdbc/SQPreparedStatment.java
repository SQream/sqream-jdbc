package com.sqream.jdbc;


import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.util.Arrays;
import java.util.List;

import javax.script.ScriptException;

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


import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorImpl;
import com.sqream.jdbc.connector.ConnectorImpl.ConnException;


public class SQPreparedStatment implements PreparedStatement {

    private ConnectorImpl Client = null;
    private SQResultSet SQRS = null;
    private SQResultSetMetaData metaData = null;
    private String sql;
    private SQConnection connection =null;
    private int statement_id;
    private String db_name;
    private int setCounter = 0;
    private int rowsInBatch = 0;
    private List<Integer> setsPerBatch = new ArrayList<>();
    private boolean is_closed = true;

    public SQPreparedStatment(Connector client, String Sql, SQConnection conn, String catalog) throws SQLException, IOException, KeyManagementException, NoSuchAlgorithmException, ScriptException, ConnException {
        
        connection = conn;
        db_name = catalog;
        is_closed = false;
        Client = new ConnectorImpl(connection.getParams().getIp(), connection.getParams().getPort(), conn.getParams().getCluster(), connection.getParams().getUseSsl());
        Client.connect(connection.getParams().getDbName(), connection.getParams().getUser(), connection.getParams().getPassword(), "sqream");  // default service
        sql = Sql;
        statement_id = Client.execute(sql);
        metaData = new SQResultSetMetaData(Client, connection.getParams().getDbName());
    }

    
    @Override
    public void close() throws SQLException {
    	//print ("inside SQPreparedStatement close");
        try {
        	if (Client!= null && Client.isOpen()) {
				if (Client.isOpenStatement()) {
					Client.close();
				}
				Client.closeConnection();
        	}
        } catch (IOException | ConnException | ScriptException e) {
        	e.printStackTrace();
            throw new SQLException(e);
        } 
        is_closed = true;
    }

    @Override
    public int[] executeBatch() throws SQLException {

        // Integer[] res = new Integer[addBatchCounter.size()];
        // res = addBatchCounter.toArray(res);  
        // int[] res = addBatchList.stream().mapToInt(i->i).toArray();  

        // Generating an array of 1's, sized by the amount of nextRow()s we issued
        
        int[] res = new int[setsPerBatch.size()];
        Arrays.fill(res, 1);
        setsPerBatch.clear();
        rowsInBatch = 0;    

        return res;
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        return SQRS;
    }

    @Override
    public void addBatch() throws SQLException {
        try {
            Client.next();
            // Update nextRow counter
            rowsInBatch++;  
            // Remember how many set commands were issued for this row in case it comes handy
            setsPerBatch.add(setCounter);  
            setCounter = 0;
        } catch (IOException | ConnException | ScriptException e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public boolean execute() {
        SQRS = new SQResultSet(Client, db_name);

        return SQRS != null;
    }
    
    @Override
    public ResultSet executeQuery() {
        SQRS = new SQResultSet(Client, db_name);
        return SQRS;
    }

    // set()
    // ----
    
    @Override
    public void setBoolean(int arg0, boolean arg1) {
        
        try {
			Client.set_boolean(arg0, arg1);
		} catch (ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++;      
    }

    @Override
    public void setByte(int arg0, byte arg1) throws SQLException {
        
    	try {
			Client.set_ubyte(arg0, arg1);
		} catch (ConnException e) {
			e.printStackTrace();
		} setCounter++; 
    }
    
    @Override
    public void setShort(int arg0, short arg1) throws SQLException {
	    try {
			Client.set_short(arg0, arg1);
		} catch (ConnException e) {
			e.printStackTrace();
		} setCounter++;    
    }

    @Override
    public void setInt(int arg0, int arg1) throws SQLException {

    	try {
			Client.set_int(arg0, arg1);
		} catch (ConnException e) {
			e.printStackTrace();
		} setCounter++;    
    }

    @Override
    public void setLong(int arg0, long arg1) throws SQLException {
        
        try {
			Client.set_long(arg0, arg1);
		} catch (ConnException e) {
			e.printStackTrace();
		} setCounter++; 
    }
    
    @Override
    public void setFloat(int arg0, float arg1) throws SQLException {
        try {
			Client.set_float(arg0, arg1);
		} catch (ConnException e) {
			e.printStackTrace();
		} setCounter++;    
    }
    
    @Override
    public void setDouble(int arg0, double arg1) throws SQLException {
        
         try {
			Client.set_double(arg0, arg1);
		} catch (ConnException e) {
			e.printStackTrace();
		} setCounter++;  
    }
    
    @Override
    public void setDate(int colNum, Date date) throws SQLException {
        try {
			Client.set_date(colNum, date);
		} catch (UnsupportedEncodingException | ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++; 
    }

    @Override
    public void setDate(int colNum, Date date, Calendar cal) throws SQLException {
    	
    	try {
			Client.set_date(colNum, date, cal.getTimeZone().toZoneId());
		} catch (UnsupportedEncodingException | ConnException e) {
			e.printStackTrace();
		} setCounter++; 
    }
    
    @Override
    public void setTimestamp(int colNum, Timestamp datetime) throws SQLException {
        
    	try {
			Client.set_datetime(colNum, datetime);
		} catch (UnsupportedEncodingException | ConnException e) {
			e.printStackTrace();
		} setCounter++; 
    }

    @Override
    public void setTimestamp(int colNum, Timestamp datetime, Calendar cal) throws SQLException {
        
    	try {
			Client.set_datetime(colNum, datetime, cal.getTimeZone().toZoneId());
		} catch (UnsupportedEncodingException | ConnException e) {
			e.printStackTrace();
		} setCounter++; 
		
		// Original logic
    	// ZonedDateTime zonedDate = datetime.toInstant().atZone(cal.getTimeZone().toZoneId()); 
		// Client.set_datetime(colNum, Timestamp.valueOf(zonedDate.toLocalDateTime()));
		// Client.set_date(colNum, Date.valueOf(zonedDate.toLocalDate()));
    }
    
    @Override
    public void setString(int arg0, String arg1) throws SQLException {
        
    	String type = getMetaData().getColumnTypeName(arg0);
        if (type.equals("Varchar"))
			try {
				Client.set_varchar(arg0, arg1);
			} catch (ConnException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		else if (type.equals("NVarchar"))
			try {
				Client.set_nvarchar(arg0, arg1);
			} catch (UnsupportedEncodingException | ConnException e) {
				e.printStackTrace();
			}
        
        setCounter++;   
    }

    @Override
    public void setNString(int arg0, String arg1) throws SQLException {
        try {
			Client.set_nvarchar(arg0, arg1);
		} catch (UnsupportedEncodingException | ConnException e) {
			e.printStackTrace();
		} setCounter++; 
    }
    
    @Override
    public void setNull(int arg0, int arg1) throws SQLException {
        
    	String type = "";
    	
    	try {
    	    type = Client.get_col_type(arg0);
    	  	
    	    if (type.equals("ftBool")) 
    	  		Client.set_boolean(arg0, null);
      		else if (type.equals("ftUByte"))
      			Client.set_ubyte(arg0, null);
      		else if (type.equals("ftShort")) 
      			Client.set_short(arg0, null);
      		else if (type.equals("ftInt")) 	
      			Client.set_int(arg0, null);
      		else if (type.equals("ftLong")) 
      			Client.set_long(arg0, null);
      		else if (type.equals("ftFloat")) 
      			Client.set_float(arg0, null);
      		else if (type.equals("ftDouble"))
      			Client.set_double(arg0, null);
      		else if (type.equals("ftDate")) 
      			Client.set_date(arg0, null);
      		else if (type.equals("ftDateTime"))
      			Client.set_datetime(arg0, null);
      		else if (type.equals("ftVarchar")) 
      			Client.set_varchar(arg0, null);
      		else if (type.equals("ftBlob")) 	
      			Client.set_nvarchar(arg0, null);
  		
    	    setCounter++;   
        } catch (ConnException | UnsupportedEncodingException e) {
			e.printStackTrace();
		} 
    }

    @Override
    public void setObject(int colIndex, Object value) throws SQLException {
        
        if (value instanceof Boolean) {
            setBoolean(colIndex, ((Boolean) value).booleanValue());
        } else if (value instanceof Byte) {
            setByte(colIndex, ((Byte) value).byteValue());
        } else if (value instanceof Short) {
            setShort(colIndex, ((Short) value).shortValue());
        } else if (value instanceof Integer) {
            setInt(colIndex, ((Integer) value).intValue());
        } else if (value instanceof Long) {
            setLong(colIndex, ((Long) value).longValue());
        } else if (value instanceof Float) {
            setFloat(colIndex, ((Float) value).floatValue());
        } else if (value instanceof Double) {
            setDouble(colIndex, ((Double) value).doubleValue());
        }  else if (value instanceof Date) {
            setDate(colIndex, (Date) value);
        } else if (value instanceof Timestamp) {
            setTimestamp(colIndex, (Timestamp) value);
        } else if (value instanceof String) {
            setString(colIndex, (String) value);
        } else 
            throw new SQLException("Type for setObject not supported");
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
        return false;
    }

    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        return false;
    }
    
    @Override
    public int executeUpdate(String arg0) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String arg0, int arg1) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        return 0;
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }
    
    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
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
        return 0;
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
    	return is_closed;
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
        return new SQParameterMetaData(Client);
    }
    
    // Unsupported
    // -----------
    
    @Override
    public void setQueryTimeout(int arg0) throws SQLException {
        if (arg0 !=0)  // 0 means unlimited timeout
            throw new SQLFeatureNotSupportedException("setQueryTimeout in SQPreparedStatement");
    }
    
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
    
    @Override
    public void setMaxRows(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException("setMaxRows in SQPreparedStatement");

        // if (arg0 != 0) // if zero, use default
        // serverProcess_chunk_size = arg0;
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
