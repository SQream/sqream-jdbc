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

import com.sqream.connector.Connector;
import com.sqream.connector.Connector.ConnException;


public class SQPreparedStatment implements PreparedStatement {

    //StatementHandle stmt = null;
    private Connector Client = null;
    SQResultSet SQRS = null;
    private SQResultSetMetaData metaData = null;
    String sql;
    private SQConnection Connection =null;  
    int statement_id;
    String db_name;
    
    int setCounter = 0;
    int rowsInBatch = 0;
    List<Integer> setsPerBatch = new ArrayList<>(); 

    
    public SQPreparedStatment(Connector client, String Sql, SQConnection conn, String catalog) throws SQLException,
            IOException, KeyManagementException, NoSuchAlgorithmException, ScriptException, ConnException {
        Tuple<String, Integer> params;
        
        Connection = conn;
        db_name = catalog;
        // Should be done in connector level
        if(conn.sqlb.Cluster) {
            params = Utils.getLBConnection(Connection.sqlb.LB_ip, Connection.sqlb.LB_port);
            Connection.sqlb.ip = params.host;
            Connection.sqlb.port = params.port;
        }
        
        Client = new Connector(Connection.sqlb.ip, Connection.sqlb.port, conn.sqlb.Cluster, Connection.sqlb.Use_ssl);

        Client.connect(Connection.sqlb.DB_name, Connection.sqlb.User, Connection.sqlb.Password, "sqream");  // default service
        sql = Sql;
        statement_id = Client.execute(sql);
        metaData = new SQResultSetMetaData(Client, Connection.sqlb.DB_name);

    }

    @Override   
    public void addBatch(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLException("Not supported");
    }

    @Override
    public void cancel() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        // TODO Auto-generated method stub

        try {
            Client.close();
        } catch (IOException | ScriptException e) {
            // TODO Auto-generated catch block
            throw new SQLException(e);
        } 
    }

    @Override
    public boolean execute(String arg0) throws SQLException {
        // TODO Auto-generated method stub

        return false;
    }

    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub

        return false;
    }

    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        // TODO Auto-generated method stub

        return false;
    }

    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        // TODO Auto-generated method stub

        return false;
    }

    /*
     * currently, we dont support bulk insert
     */
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
    public ResultSet executeQuery(String arg0) throws SQLException {
        // TODO Auto-generated method stub

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String arg0) throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public int executeUpdate(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public Connection getConnection() throws SQLException {
        // TODO Auto-generated method stub

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public int getFetchSize() throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // TODO Auto-generated method stub

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public int getMaxRows() throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        // TODO Auto-generated method stub

        return false;
    }

    @Override
    public boolean getMoreResults(int arg0) throws SQLException {
        // TODO Auto-generated method stub

        return false;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return SQRS;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        // TODO Auto-generated method stub

        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub

        return false;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setCursorName(String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setEscapeProcessing(boolean arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setFetchDirection(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
       if (rows <0)
           throw new SQLException("setFetchSize argument should be nonnegative, got " + rows);
    }

    @Override
    public void setMaxFieldSize(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setMaxRows(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException();

        // if (arg0 != 0) // if zero, use default
        // serverProcess_chunk_size = arg0;

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
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLFeatureNotSupportedException();
    }

    //
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
            // TODO Auto-generated catch block
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            SQRS = new SQResultSet(Client, db_name);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        return SQRS != null;
    }
    

    @Override
    public ResultSet executeQuery() throws SQLException {
        // TODO Auto-generated method stub

        try {
            SQRS = new SQResultSet(Client, db_name);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return SQRS;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException ("Not supported");

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        
        if (metaData == null)
            throw new SQLException("MetaData is empty");
        return metaData;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        // TODO Auto-generated method stub
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int arg0, Array arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int arg0, Blob arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBlob(int arg0, InputStream arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBoolean(int arg0, boolean arg1) throws SQLException {
        // TODO Auto-generated method stub
        
        try {
			Client.set_boolean(arg0, arg1);
		} catch (ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++;      
        
    }

    @Override
    public void setByte(int arg0, byte arg1) throws SQLException {
        // TODO Auto-generated method stub
        try {
			Client.set_ubyte(arg0, arg1);
		} catch (ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++; 
        
    }

    @Override
    public void setBytes(int arg0, byte[] arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int arg0, Clob arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int arg0, Reader arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int arg0, Date arg1) throws SQLException {
        // TODO Auto-generated method stub
        try {
			Client.set_date(arg0, arg1);
		} catch (UnsupportedEncodingException | ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++; 
    }

    @Override
    public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
        // TODO Auto-generated method stub
        try {
			Client.set_date(arg0, arg1);
		} catch (UnsupportedEncodingException | ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++; 
    }

    static int convertDateToInt(java.sql.Date Output) {
        int result = 0;
        if (Output != null) {
            Calendar c = Calendar.getInstance();
            c.setTime(Output);
            int year = c.get(Calendar.YEAR);
            int month = c.get(Calendar.MONTH) + 1;
            int day = c.get(Calendar.DAY_OF_MONTH);

            month = (month + 9) % 12;
            year = year - month / 10;

            result = (365 * year + year / 4 - year / 100 + year / 400 + (month * 306 + 5) / 10 + (day - 1));
        }
        return result;

    }

    @Override
        public void setDouble(int arg0, double arg1) throws SQLException {
        
         try {
			Client.set_double(arg0, arg1);
		} catch (ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++;  
       }

    @Override
    public void setFloat(int arg0, float arg1) throws SQLException {
        // TODO Auto-generated method stub
        try {
			Client.set_float(arg0, arg1);
		} catch (ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++;    
    }

    @Override
    public void setInt(int arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
          try {
			Client.set_int(arg0, arg1);
		} catch (ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++;    
        
        
    }

    @Override
    public void setLong(int arg0, long arg1) throws SQLException {
        // TODO Auto-generated method stub
        
            //SqreamLog.writeInfo("", Client);
            try {
				Client.set_long(arg0, arg1);
			} catch (ConnException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} setCounter++; 
        
            }

    @Override
    public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int arg0, NClob arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNClob(int arg0, Reader arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNString(int arg0, String arg1) throws SQLException {
        // TODO Auto-generated method stub
        try {
			Client.set_nvarchar(arg0, arg1);
		} catch (UnsupportedEncodingException | ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++; 
    }

    @Override
    public void setNull(int arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        String type = "";
        // SqrmTypes sqrmType = SqrmTypes.getSqreamTypeBySqlType(arg1);
        
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }

    @Override
    public void setNull(int arg0, int arg1, String arg2) throws SQLException {
        
    }

    /**
     * get a timeStamp, extract a date & a time from it(into int values), and
     * then: put those 2 int values into 1 long value.
     * 
     * @param ts
     * @return
     */
    static long convertTimeStampToLong(java.sql.Timestamp ts) {
        return DatetimeLogic.convertTimeStampToLong(ts);
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

    @Override
    public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setObject(int arg0, Object arg1, int arg2, int arg3) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setRef(int arg0, Ref arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int arg0, RowId arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setShort(int arg0, short arg1) throws SQLException {
    try {
		Client.set_short(arg0, arg1);
	} catch (ConnException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} setCounter++;    

    }

    @Override
    public void setString(int arg0, String arg1) throws SQLException {
        String type = getMetaData().getColumnTypeName(arg0);
        if (type.equals("Varchar"))
			try {
				Client.set_varchar(arg0, arg1);
			} catch (ConnException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		else if (type.equals("NVarchar"))
			try {
				Client.set_nvarchar(arg0, arg1);
			} catch (UnsupportedEncodingException | ConnException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        setCounter++;   
    }

    @Override
    public void setTime(int arg0, Time arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();

        // sqreamStorage.SetTuppleValue(SqrmTypes.ftDateTime, arg0-1, arg1);
    }

    @Override
    public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
        // sqreamStorage.SetTuppleValue(SqrmTypes.ftDateTime, arg0-1, arg1);
    }

    @Override
    public void setTimestamp(int arg0, Timestamp arg1) throws SQLException {
        // TODO Auto-generated method stub
        try {
			Client.set_datetime(arg0, arg1);
		} catch (UnsupportedEncodingException | ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++; 
    }

    @Override
    public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2) throws SQLException {
        try {
			Client.set_datetime(arg0, arg1);
		} catch (UnsupportedEncodingException | ConnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} setCounter++; 

    }

    @Override
    public void setURL(int arg0, URL arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setUnicodeStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    /*
     * public QueryType[] getResType() { return QType; } public void
     * setResType(QueryType[] QType) { this.QType = QType; }
     */
    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

}
