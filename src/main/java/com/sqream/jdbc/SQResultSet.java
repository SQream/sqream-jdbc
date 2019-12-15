package com.sqream.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import javax.script.ScriptException;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.enums.RS_STAT;

class SQResultSet implements ResultSet {

	private Connector client = null;
	private RS_STAT status = RS_STAT.CLOSE;
	private int maxRows = 0;
	private String dbName;
	private boolean empty = false;
	private boolean removeSpaces = false;
	private boolean isNull = true;
	private boolean isClosed = true;

	SQResultSet(Connector client, String catalog) {
		this(client, catalog, false);
	}
	
	SQResultSet(Connector client, String catalog, boolean removeSpaces) {
		this.client = client;
		this.status = RS_STAT.OPEN;
		this.dbName = catalog;
		this.removeSpaces = removeSpaces;
	    this.isClosed = false;
	}

	SQResultSet(boolean empty) {
		this.empty = empty;
	}

	/**
	 * Retrieves the number, types and properties of this ResultSet object's columns.
	 */
	@Override
	public ResultSetMetaData getMetaData() {
		try {
			return new SQResultSetMetaData(client, dbName);
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Moves the cursor forward one row from its current position.
	 */
	@Override
	public boolean next() throws SQLException {
		if (empty)
			return false;
		try {
			return this.client.next();
		} catch (Exception e2) {
			try {
				if (client != null && client.isOpen() && client.isOpenStatement()) {
					client.close();
				}
			} catch (IOException | ConnException | ScriptException e) {
				e.printStackTrace();
				throw new SQLException(e.getMessage());
			}
			e2.printStackTrace();
			throw new SQLException(e2.getMessage());
		}
	}

	/**
	 * Reports whether the last column read had a value of SQL NULL.
	 */
	@Override
	public boolean wasNull() {
		return isNull;
	}

	/**
	 * Retrieves the concurrency mode of this ResultSet object.
	 */
	@Override
	public int getConcurrency() {
		return CONCUR_READ_ONLY;
	}

	@Override
	public void updateDate(int columnIndex, Date x) {
		this.baseUsageError();
		//FIXME: where is implementation of method? Need to implement or throw UnsupportedException.
	}

	/**
	 * Retrieves whether this ResultSet object has been closed.
	 */
	@Override
	public boolean isClosed() {
		return isClosed;
	}

	/**
	 * Releases this ResultSet object's database and JDBC resources immediately
	 * instead of waiting for this to happen when it is automatically closed
	 */
	@Override
	public void close() {
		isClosed = true;
		if (!empty) {  // Empty result sets don't start with a Client
			try {
				if (client!= null && client.isOpen()) {
					if (client.isOpenStatement()) {
						client.close();
					}
					client.closeConnection();
				}
			} catch (IOException | ConnException | ScriptException e) {
				e.printStackTrace();
			}
			isClosed = true;
		}
	}

	/**
	 * Maps the given ResultSet column label to its ResultSet column index.
	 * @param columnLabel - the label for the column specified with the SQL AS clause.
	 *                      If the SQL AS clause was not specified, then the label is the name of the column
	 * @return the column index of the given column name
	 */
	@Override
	public int findColumn(String columnLabel) {
		this.baseUsageError();
		return 0;
	}

	void setMaxRows(int maxRows) {
		this.maxRows = maxRows;
	}

	void setEmpty(boolean empty) {
		this.empty = empty;
	}

	private void baseUsageError() {
		final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
		ste[2].getMethodName(); // show who called
		// "baseUsageError".
	}

	//<editor-fold desc="Getting the data of the current row">
	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		try {
			Boolean res = client.getBoolean(columnLabel.toLowerCase());
			isNull = res == null;
			return (res == null) ? false : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public boolean getBoolean(int columnIndex) {
		Boolean res = null;
		try {
			res = client.getBoolean(columnIndex);
			isNull = res == null;
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? false : res;
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		try {
			Byte res = client.get_ubyte(columnLabel.toLowerCase());
			isNull = res == null;
			return (res == null) ? 0 : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public byte getByte(int columnIndex) {

		Byte res = null;
		try {
			res = client.get_ubyte(columnIndex);
			isNull = res == null;
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? 0 : res;
	}

	@Override
	public short getShort(String columnLabel) {
		Short res = null;
		columnLabel = columnLabel.toLowerCase();
		try {
			res = client.get_short(columnLabel);
			isNull = res == null;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? 0 : res;
	}
	
	@Override
	public short getShort(int columnIndex) {
		Short res = null;
		try {
			res = client.get_short(columnIndex);
			
			isNull = res == null;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		
		return (res == null) ? 0 : res;
	}
	
	
	@Override
	public int getInt(String columnLabel) {
		Integer res = null;
		columnLabel = columnLabel.toLowerCase();
		try {
			res = client.get_int(columnLabel.toLowerCase());
			
			isNull = res == null;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return (res == null) ? 0 : res;
	}

	@Override
	public int getInt(int columnIndex) {
		Integer res = null;
		try {
			res = client.get_int(columnIndex);
			isNull = res == null;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? 0 : res;
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		try {
			Long res = client.get_long(columnLabel.toLowerCase());
			isNull = res == null;
			return (res == null) ? 0 : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("Exception on getLong:" + e.toString());
		}
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		try {
			Long res = client.get_long(columnIndex);
			isNull = res == null;
			return (res == null) ? 0 : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnIndex
					+ "' not found");
		}
	}
	
	@Override
	public float getFloat(String columnLabel) throws SQLException {
		try {
			Float res = client.get_float(columnLabel.toLowerCase());
			isNull = res == null;
			return (res == null) ? 0 : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public float getFloat(int columnIndex) {
		Float res = null;
		try {
			res = client.get_float(columnIndex);
			isNull = res == null;
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? 0 : res;
	}
	
	@Override
	public double getDouble(String columnLabel) {
		Double res = null;
		columnLabel = columnLabel.toLowerCase();
		try {
			res = client.get_double(columnLabel);
			isNull = res == null;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? 0 : res;
	}

	@Override
	public double getDouble(int columnIndex) {
		Double res = null;
		try {
			res = client.get_double(columnIndex);
			isNull = res == null;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? 0 : res;
	}
	
	@Override
	public Date getDate(int columnIndex) throws SQLException {
		try {
			Date res = client.get_date(columnIndex);
			isNull = res == null;
			return res;
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException("");
		}
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		try {
			Date res = client.get_date(columnLabel.toLowerCase());
			isNull = res == null;
			return res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		try {
			Date res = client.get_date(columnIndex, cal.getTimeZone().toZoneId());
			isNull = res == null;
			return res;
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException("");
		}
	}
	
	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		try {
			Date res = client.get_date(columnLabel.toLowerCase(), cal.getTimeZone().toZoneId());
			isNull = res == null;
			return res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}
	
	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		try {
			Timestamp res =  client.get_datetime(columnLabel.toLowerCase());
			isNull = res == null;
			return res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) {
		Timestamp res = null;
		try {
			res = client.get_datetime(columnIndex);
			isNull = res == null;
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) {
		// Original logic:
		// Instant instant = utcDateTime.toLocalDateTime().atZone(cal.getTimeZone().toZoneId()).toInstant();
		// utcDateTime = Timestamp.from(instant);
		
		Timestamp res = null;
		try {
			res = client.get_datetime(columnIndex, cal.getTimeZone().toZoneId());
			isNull = res == null;
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return res;
        
	}
	
	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		try {
			Timestamp res =  client.get_datetime(columnLabel.toLowerCase(), cal.getTimeZone().toZoneId());
			isNull = res == null;
			return res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		String res = null;
		Object res_obj;
		String type = "";
		columnLabel = columnLabel.toLowerCase();
		try {
			type = client.get_col_type(columnLabel);
		} catch (ConnException e1) {
			e1.printStackTrace();
		}
		
		if (type.equals("ftBlob")) {
			try {
				res = client.get_nvarchar(columnLabel);
			}catch (ConnException e) {
				e.printStackTrace();
			}
		}
		else if (type.equals("ftVarchar")) {
			try {
				res = client.get_varchar(columnLabel);
			}catch (ConnException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		else {
			res_obj = getObject(columnLabel);
			res = (res_obj == null) ? null : res_obj.toString();
		}
		isNull = res == null;
		return res;
	}
	
	@Override
	public String getString(int columnIndex) throws SQLException {
		String res = null;
		Object res_obj;
		String type = "";
		try {
			type = client.get_col_type(columnIndex);
		} catch (ConnException e1) {
			e1.printStackTrace();
		}
		
		if (type.equals("ftBlob")) {
			try {
				res = client.get_nvarchar(columnIndex);
			}catch (ConnException e) {
				e.printStackTrace();
			}
		}
		else if (type.equals("ftVarchar")) {
			try {
				res = client.get_varchar(columnIndex);
			}catch (ConnException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		else {
			res_obj = getObject(columnIndex);
			res = (res_obj == null) ? null : res_obj.toString();
		}
		isNull = res == null;
		return res;
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		
		columnLabel = columnLabel.toLowerCase();
		String type = "";
		Object res = null; 
		
		try {
			type = client.get_col_type(columnLabel);
		} catch (ConnException e) {
			e.printStackTrace();
		}
		
		if (type.equals("ftBool")) 
			res = getBoolean(columnLabel);
		else if (type.equals("ftUByte"))
			res = getByte(columnLabel);
		else if (type.equals("ftShort")) 
			res = getShort(columnLabel);
		else if (type.equals("ftInt")) 	
			res = getInt(columnLabel);
		else if (type.equals("ftLong")) 
			res = getLong(columnLabel);
		else if (type.equals("ftFloat")) 
			res = getFloat(columnLabel);
		else if (type.equals("ftDouble"))
			res = getDouble(columnLabel);
		else if (type.equals("ftDate")) 
			res = getDate(columnLabel);
		else if (type.equals("ftDateTime"))
			res = getTimestamp(columnLabel);
		else if (type.equals("ftVarchar")) 
			res = getString(columnLabel);
		else if (type.equals("ftBlob")) 	
			res = getString(columnLabel);
		
		return (isNull) ? null : res;  

	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		
		String type = "";
		Object res = null; 
		
		try {
			type = client.get_col_type(columnIndex);
		} catch (ConnException e) {
			e.printStackTrace();
		}
		
		if (type.equals("ftBool")) 
			res = getBoolean(columnIndex);
		else if (type.equals("ftUByte"))
			res = getShort(columnIndex);
		else if (type.equals("ftShort"))
			res = getShort(columnIndex);
		else if (type.equals("ftInt"))
			res = getInt(columnIndex);
		else if (type.equals("ftLong")) 
			res = getLong(columnIndex);
		else if (type.equals("ftFloat")) 
			res = getFloat(columnIndex);
		else if (type.equals("ftDouble"))
			res = getDouble(columnIndex);
		else if (type.equals("ftDate")) 
			res = getDate(columnIndex);
		else if (type.equals("ftDateTime"))
			res = getTimestamp(columnIndex);
		else if (type.equals("ftVarchar")) 
			res = getString(columnIndex);
		else if (type.equals("ftBlob")) 	
			res = getString(columnIndex);
		
		return (isNull) ? null : res;
	}
	//</editor-fold>

	//<editor-fold desc="Unsupported">
	// Unsupported
	// -----------

	@Override
	public boolean previous() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("previous in SQResultSet");
	}

	@Override
	public Statement getStatement() throws SQLException {
		throw new SQLFeatureNotSupportedException("getStatement in SQResultSet");
	}
	
	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBytes in SQResultSet");
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBytes 2 in SQResultSet");
	}
	
	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBigDecimal in SQResultSet");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("unwrap in SQResultSet");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("isWrapperFor in SQResultSet");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getBigDecimal in SQResultSet");
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getTime in SQResultSet");
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getAsciiStream in SQResultSet");
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getUnicodeStream in SQResultSet");
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBinaryStream in SQResultSet");
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getBigDecimal in SQResultSet");
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getTime in SQResultSet");
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getAsciiStream 2 in SQResultSet");
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getUnicodeStream 2 in SQResultSet");
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("shoko in SQResultSet");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getWarnings in SQResultSet");
	}

	@Override
	public void clearWarnings() throws SQLException {

		this.baseUsageError(); 
		throw new SQLFeatureNotSupportedException("clearWarnings in SQResultSet");
	}

	@Override
	public String getCursorName() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getCursorName in SQResultSet");
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getCharacterStream in SQResultSet");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getCharacterStream 2 in SQResultSet");
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getBigDecimal in SQResultSet");
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("isBeforeFirst in SQResultSet");
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("isAfterLast in SQResultSet");
	}

	@Override
	public boolean isFirst() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("isFirst in SQResultSet");
	}

	@Override
	public boolean isLast() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("isLast in SQResultSet");
	}

	@Override
	public void beforeFirst() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("beforeFirst in SQResultSet");

	}

	@Override
	public void afterLast() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("afterLast in SQResultSet");
	}

	@Override
	public boolean first() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("first in SQResultSet");
	}

	@Override
	public boolean last() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("last in SQResultSet");
	}

	@Override
	public int getRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getRow in SQResultSet");
	}

	@Override
	public boolean absolute(int row) {
		this.baseUsageError();
		return false;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("relative in SQResultSet");
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("setFetchDirection in SQResultSet");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getFetchDirection in SQResultSet");
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("setFetchSize in SQResultSet");
	}

	@Override
	public int getFetchSize() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getFetchSize in SQResultSet");
	}

	@Override
	public int getType() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getType in SQResultSet");
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("rowUpdated in SQResultSet");
	}

	@Override
	public boolean rowInserted() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("rowInserted in SQResultSet");
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("rowDeleted in SQResultSet");
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNull in SQResultSet");
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBoolean in SQResultSet");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateByte in SQResultSet");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateShort in SQResultSet");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateInt in SQResultSet");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateLong in SQResultSet");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateFloat in SQResultSet");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateDouble in SQResultSet");
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBigDecimal in SQResultSet");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateString in SQResultSet");
	}	

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBytes in SQResultSet");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateTime in SQResultSet");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateTimestamp in SQResultSet");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("SQLException in SQResultSet");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBinaryStream in SQResultSet");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateCharacterStream in SQResultSet");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateObject in SQResultSet");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateObject 2 in SQResultSet");
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNull in SQResultSet");
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBoolean in SQResultSet");
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateByte 2 in SQResultSet");
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateShort in SQResultSet");
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateInt in SQResultSet");
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateLong in SQResultSet");
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateFloat in SQResultSet");
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateDouble in SQResultSet");
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBigDecimal in SQResultSet");
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateString in SQResultSet");
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBytes in SQResultSet");
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateDate in SQResultSet");
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateTime in SQResultSet");
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateTimestamp in SQResultSet");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateAsciiStream 2 in SQResultSet");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBinaryStream 2 in SQResultSet");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateCharacterStream 2 in SQResultSet");
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateObject in SQResultSet");
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateObject 2 in SQResultSet");
	}

	@Override
	public void insertRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("insertRow in SQResultSet");
	}

	@Override
	public void updateRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateRow in SQResultSet");
	}

	@Override
	public void deleteRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("deleteRow in SQResultSet");
	}

	@Override
	public void refreshRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("refreshRow in SQResultSet");
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("cancelRowUpdates in SQResultSet");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("moveToInsertRow in SQResultSet");
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("moveToCurrentRow in SQResultSet");
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getObject in SQResultSet");
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getRef in SQResultSet");
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getBlob in SQResultSet");
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getClob in SQResultSet");
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getArray in SQResultSet");
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getObject in SQResultSet");
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getObject in SQResultSet");
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getBlob 2 in SQResultSet");
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getClob 2 in SQResultSet");
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getArray in SQResultSet");
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getTime in SQResultSet");
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getTime 2 in SQResultSet");
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getURL in SQResultSet");
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getURL 2 in SQResultSet");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateRef in SQResultSet");
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateRef 2 in SQResultSet");
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBlob in SQResultSet");
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBlob 2 in SQResultSet");
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateClob in SQResultSet");
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateClob 2 in SQResultSet");
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateArray 2 in SQResultSet");
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateArray 3 in SQResultSet");
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getRowId in SQResultSet");
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getRowId 2 in SQResultSet");
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateRowId in SQResultSet");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateRowId 2 in SQResultSet");
	}

	@Override
	public int getHoldability() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getHoldability in SQResultSet");
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNString in SQResultSet");
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNString 2 in SQResultSet");
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNClob in SQResultSet");
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNClob 2 in SQResultSet");
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getNClob in SQResultSet");
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getNClob in SQResultSet");
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getSQLXML in SQResultSet");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getSQLXML 2 in SQResultSet");
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateSQLXML in SQResultSet");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateSQLXML 2 in SQResultSet");
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		this.baseUsageError();
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getNString in SQResultSet");
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		this.baseUsageError();
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getNString 2 in SQResultSet");
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getNCharacterStream in SQResultSet");
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("getNCharacterStream 2 in SQResultSet");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNCharacterStream in SQResultSet");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNCharacterStream 2 in SQResultSet");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateAsciiStream in SQResultSet");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBinaryStream in SQResultSet");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateCharacterStream in SQResultSet");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateAsciiStream 2 in SQResultSet");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBinaryStream 2 in SQResultSet");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateCharacterStream 2 in SQResultSet");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBlob 4 in SQResultSet");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBlob 5 in SQResultSet");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateClob 4 in SQResultSet");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateClob 5 in SQResultSet");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateClob 4 in SQResultSet");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNClob 5 in SQResultSet");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNCharacterStream in SQResultSet");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNCharacterStream in SQResultSet");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateAsciiStream in SQResultSet");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBinaryStream in SQResultSet");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateCharacterStream in SQResultSet");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateAsciiStream in SQResultSet");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBinaryStream in SQResultSet");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateCharacterStream in SQResultSet");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBlob in SQResultSet");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateBlob in SQResultSet");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateClob in SQResultSet");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateClob in SQResultSet");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNClob in SQResultSet");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException("updateNClob in SQResultSet");
	}

	@Override
	public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getObject in SQResultSet");
	}

	@Override
	public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getObject in SQResultSet");
	}
	//</editor-fold>
}
