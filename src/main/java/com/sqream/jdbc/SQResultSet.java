package com.sqream.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

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


import java.time.Instant;
//import java.time.ZoneId;
//import java.time.OffsetDateTime;   // For OffsetDateTime.now().getOffset() to get the machine's offset
//import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import com.sqream.jdbc.Connector;
import com.sqream.jdbc.Connector.ConnException;



class SQResultSet implements ResultSet {

	
	Connector Client = null;
	RS_STAT Status = RS_STAT.CLOSE;
	int MaxRows = 0;
	String db_name;
	
	boolean Empty = false;
	boolean RemoveSpaces = false;
	boolean isNull = true;
    boolean is_closed = true;

    
	static void print(Object printable) {
        System.out.println(printable);
    }
	
	enum RS_STAT {
		BASE, FETCH_ONCE, FINISH, CLOSE, OPEN
	}

	boolean MetaDataResultSet = false;

	enum CATALOG_columns {
		TABLE_CAT, NONE
	}

	enum TABLE_columns {
		TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, NONE
	} // there are more columns..

	enum COLUMN_columns {
		TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, NONE
	} // there are more columns..

	enum TABLE_types {
		TABLE_TYPE, NONE
	} // there are more columns..

	
	public void baseUsageError() throws SQLException {

		final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
		ste[2].getMethodName(); // show who called
								// "baseUsageError".

	}	
	
	public SQResultSet(String catalog) throws IOException {
		this(null, catalog, false);
	}

	public SQResultSet(Connector client, String catalog)
			throws IOException {
		this(client, catalog, false);
	}
	
	public SQResultSet(String catalog, boolean removeSpaces)
			throws IOException {
		this(null, catalog, removeSpaces);
		
	}
	
	public SQResultSet( Connector client, String catalog, boolean removeSpaces)
			throws IOException {
		Client = client;
		Status = RS_STAT.OPEN;
		db_name = catalog;
		RemoveSpaces = removeSpaces;
	    is_closed = false;

	}
	
	
	public SQResultSet(boolean bEmpty) throws IOException {
		
		Empty = bEmpty;

	}

	public SQResultSet() throws IOException {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void close() throws SQLException {
		
		is_closed = true;
		/*
		if (!Empty) {  // Empty result sets don't start with a Client
			try {
				if (Client!= null && Client.is_open()) {
					if (Client.is_open_statement()) 
						Client.close();
					Client.close_connection();
				}
			} catch (IOException | ConnException | ScriptException e) {
				e.printStackTrace();
			}
		    is_closed = true;
		}
		//*/
	}

	@Override
	public Statement getStatement() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		try {
			Boolean res = Client.get_boolean(columnLabel.toLowerCase());
			isNull = (res == null) ? true : false;
			return (res == null) ? false : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		Boolean res = null;
		try {
			res = Client.get_boolean(columnIndex);
			isNull = (res == null) ? true : false;
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? false : res;

	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		try {
			Byte res = Client.get_ubyte(columnLabel.toLowerCase());
			isNull = (res == null) ? true : false;
			return (res == null) ? 0 : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {

		Byte res = null;
		try {
			res = Client.get_ubyte(columnIndex);
			isNull = (res == null) ? true : false;
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? 0 : res;
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		Short res = null;
		String type = "";
		columnLabel = columnLabel.toLowerCase();
		try {
			/*
			type = Client.get_col_type(columnLabel);
			if (type.equals("ftUByte")) 
				res = (short) (Client.get_ubyte(columnLabel) & 0xFF);
			else //*/ 
			res = Client.get_short(columnLabel);
			
			isNull = (res == null) ? true : false;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		
		return (res == null) ? 0 : res;
	}
	
	@Override
	public short getShort(int columnIndex) throws SQLException {
		Short res = null;
		String type = "";
		try {
			/*
			type = Client.get_col_type(columnIndex);
			if (type.equals("ftUByte")) 
				res = (short) (Client.get_ubyte(columnIndex)  & 0xFF);
			else //*/ 
			res = Client.get_short(columnIndex);
			
			isNull = (res == null) ? true : false;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		
		return (res == null) ? 0 : res;
	}
	
	
	@Override
	public int getInt(String columnLabel) throws SQLException {
		Integer res = null;
		String type = "";
		columnLabel = columnLabel.toLowerCase();
		try {
			/*
			type = Client.get_col_type(columnLabel);
			if (type.equals("ftUByte")) 
				res = (int) (Client.get_ubyte(columnLabel) & 0xFF);
			else if (type.equals("ftShort")) 
				res = (int)Client.get_short(columnLabel);
			else //*/ 
			res = Client.get_int(columnLabel.toLowerCase());
			
			isNull = (res == null) ? true : false;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return (res == null) ? 0 : res;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		Integer res = null;
		String type = "";
		try {
			/*
			type = Client.get_col_type(columnIndex);
			if (type.equals("ftUByte")) 
				res = (int) (Client.get_ubyte(columnIndex) & 0xFF);
			else if (type.equals("ftShort")) 
				res = (int)Client.get_short(columnIndex);
			else //*/ 
			res = Client.get_int(columnIndex);
			
			isNull = (res == null) ? true : false;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		
		return (res == null) ? 0 : res;
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		try {
			Long res = Client.get_long(columnLabel.toLowerCase());
			isNull = (res == null) ? true : false;
			return (res == null) ? 0 : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("Exception on getLong:" + e.toString());
		}
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		try {
			Long res = Client.get_long(columnIndex);
			isNull = (res == null) ? true : false;
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
			Float res = Client.get_float(columnLabel.toLowerCase());
			isNull = (res == null) ? true : false;
			return (res == null) ? 0 : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		Float res = null;
		try {
			res = Client.get_float(columnIndex);
			isNull = (res == null) ? true : false;
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? 0 : res;

	}
	
	@Override
	public double getDouble(String columnLabel) throws SQLException {
		Double res = null;
		String type = "";
		columnLabel = columnLabel.toLowerCase();
		try {
			/*
			type = Client.get_col_type(columnLabel);
			if (type.equals("ftFloat")) 
				res = (double) Client.get_float(columnLabel);
			else if (type.equals("ftUByte")) 
				res = (double) Client.get_ubyte(columnLabel);
			else if (type.equals("ftShort")) 
				res = (double) Client.get_short(columnLabel);
			else if (type.equals("ftInt")) 
				res = (double) Client.get_int(columnLabel);
			else if (type.equals("ftLong")) 
				res = (double) Client.get_long(columnLabel);
			else //*/  
			res = Client.get_double(columnLabel);
			
			isNull = (res == null) ? true : false;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		
		return (res == null) ? 0 : res;
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		Double res = null;
		String type = "";
		try {
			/*
			type = Client.get_col_type(columnIndex);
			if (type.equals("ftFloat")) 
				res = (double) Client.get_float(columnIndex);
			else if (type.equals("ftUByte")) 
				res = (double) Client.get_ubyte(columnIndex);
			else if (type.equals("ftShort")) 
				res = (double) Client.get_short(columnIndex);
			else if (type.equals("ftInt")) 
				res = (double) Client.get_int(columnIndex);
			else if (type.equals("ftLong")) 
				res = (double) Client.get_long(columnIndex);
			else //*/  
			res = Client.get_double(columnIndex);
			
			isNull = (res == null) ? true : false;
		} 
		catch (ConnException e) {
			e.printStackTrace();
		}
		
		return (res == null) ? 0 : res;
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		try {
			Date res = Client.get_date(columnLabel.toLowerCase());
			isNull = (res == null) ? true : false;
			return (res == null) ? null : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		try {
			Date res = Client.get_date(columnIndex);
			isNull = (res == null) ? true : false;
			return (res == null) ? null : res;
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException("");
		}
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw  new SQLFeatureNotSupportedException("getDate with calendar parameter");
        /*
		try {
			Date res = Client.get_date(columnLabel.toLowerCase());
			if (res != null) {
				ZonedDateTime zonedDate = Instant.ofEpochMilli(res.getTime()).atZone(cal.getTimeZone().toZoneId());  
				res = Date.valueOf(zonedDate.toLocalDate());
			}
			isNull = (res == null) ? true : false;
			return (res == null) ? null : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
		//*/
	}
	
	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw  new SQLFeatureNotSupportedException("getDate with calendar parameter");
        /*
		try {
			Date res = Client.get_date(columnIndex);
			if (res != null) {
				ZonedDateTime zonedDate = Instant.ofEpochMilli(res.getTime()).atZone(cal.getTimeZone().toZoneId());  
				res = Date.valueOf(zonedDate.toLocalDate());
			}
			isNull = (res == null) ? true : false;
			return (res == null) ? null : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("");
		}
		//*/
	}
	
	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		try {
			Timestamp res =  Client.get_datetime(columnLabel.toLowerCase());
			isNull = (res == null) ? true : false;
			return (res == null) ? null : res;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException("columnLabel '" + columnLabel.trim()
					+ "' not found");
		}
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Timestamp res = null;
		try {
			res = Client.get_datetime(columnIndex);
			isNull = (res == null) ? true : false;
		} catch (ConnException e) {
			e.printStackTrace();
		}
		return (res == null) ? null : res;

	}
	
	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw  new SQLFeatureNotSupportedException("getTimestamp with calendar parameter");
        /*
		try {
			Timestamp utcDateTime = Client.get_datetime(columnIndex);
			if (utcDateTime!= null) {
				Instant instant = utcDateTime.toLocalDateTime().atZone(cal.getTimeZone().toZoneId()).toInstant();
				utcDateTime = Timestamp.from(instant);
			}
			isNull = (utcDateTime == null) ? true : false;
			return (utcDateTime == null) ? null : utcDateTime;
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException("");
		}
		//*/
	}
	
	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw  new SQLFeatureNotSupportedException("getTimestamp with calendar parameter");

		/*
		try {
			Timestamp utcDateTime = Client.get_datetime(columnLabel.toLowerCase());
			if (utcDateTime!= null) {
				Instant instant = utcDateTime.toLocalDateTime().atZone(cal.getTimeZone().toZoneId()).toInstant();
				utcDateTime = Timestamp.from(instant);
			}
			isNull = (utcDateTime == null) ? true : false;
			return (utcDateTime == null) ? null : utcDateTime;
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException("");
		}
		//*/
	}
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		String res = null;
		Object res_obj =null;
		String type = "";
		columnLabel = columnLabel.toLowerCase();
		try {
			type = Client.get_col_type(columnLabel);
		} catch (ConnException e1) {
			e1.printStackTrace();
		}
		
		if (type.equals("ftBlob")) {
			try {
				res = Client.get_nvarchar(columnLabel);
			}catch (ConnException e) {
				e.printStackTrace();
			}
		}
		else if (type.equals("ftVarchar")) {
			try {
				res = Client.get_varchar(columnLabel);
			}catch (ConnException e) {
				e.printStackTrace();
			}
		}
		else {
			res_obj = getObject(columnLabel);
			res = (res_obj == null) ? null : res_obj.toString();
		}
		
		
		if (RemoveSpaces && res != null) 
			res = res.trim();
		
		
		isNull = (res == null) ? true : false;
		
		return (res == null) ? null : res;
	}
	
	@Override
	public String getString(int columnIndex) throws SQLException {
		String res = null;
		Object res_obj =null;
		String type = "";
		try {
			type = Client.get_col_type(columnIndex);
		} catch (ConnException e1) {
			e1.printStackTrace();
		}
		
		if (type.equals("ftBlob")) {
			try {
				res = Client.get_nvarchar(columnIndex);
			}catch (ConnException e) {
				e.printStackTrace();
			}
		}
		else if (type.equals("ftVarchar")) {
			try {
				res = Client.get_varchar(columnIndex);
			}catch (ConnException e) {
				e.printStackTrace();
			}
		}
		else {
			res_obj = getObject(columnIndex);
			res = (res_obj == null) ? null : res_obj.toString();
		}

		if (RemoveSpaces && res != null) 
			res = res.trim();
		
		if (type.equals("ftBool")) 
			res = res.equals("0") ? "false" : "true";
		
		isNull = (res == null) ? true : false;
		
		return (res == null) ? null : res;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		ResultSetMetaData rsmd = null;
		try {
			rsmd = new SQResultSetMetaData(Client, db_name);
		} catch (IOException | ConnException e) {
			e.printStackTrace();
		}
		return rsmd;
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		
		columnLabel = columnLabel.toLowerCase();
		String type = "";
		Object res = null; 
		
		try {
			type = Client.get_col_type(columnLabel);
		} catch (ConnException e) {
			e.printStackTrace();
		}
		
		if (type.equals("ftBool")) 
			res =  (Boolean)getBoolean(columnLabel);
		else if (type.equals("ftUByte"))
			res =  (Byte)getByte(columnLabel);
		else if (type.equals("ftShort")) 
			res =  (Short)getShort(columnLabel);
		else if (type.equals("ftInt")) 	
			res =  (Integer)getInt(columnLabel);
		else if (type.equals("ftLong")) 
			res =  (Long)getLong(columnLabel);
		else if (type.equals("ftFloat")) 
			res =  (Float)getFloat(columnLabel);
		else if (type.equals("ftDouble"))
			res =  (Double)getDouble(columnLabel);
		else if (type.equals("ftDate")) 
			res =  (Date)getDate(columnLabel);
		else if (type.equals("ftDateTime"))
			res =  (Timestamp)getTimestamp(columnLabel);
		else if (type.equals("ftVarchar")) 
			res =  (String)getString(columnLabel);
		else if (type.equals("ftBlob")) 	
			res =  (String)getString(columnLabel);
		
		return (isNull) ? null : res;  

	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		
		String type = "";
		Object res = null; 
		
		try {
			type = Client.get_col_type(columnIndex);
		} catch (ConnException e) {
			e.printStackTrace();
		}
		
		if (type.equals("ftBool")) 
			res =  (Boolean)getBoolean(columnIndex);
		else if (type.equals("ftUByte"))
			res =  (Short)getShort(columnIndex);
		else if (type.equals("ftShort")) 
			res =  (Short)getShort(columnIndex);
		else if (type.equals("ftInt")) 	
			res =  (Integer)getInt(columnIndex);
		else if (type.equals("ftLong")) 
			res =  (Long)getLong(columnIndex);
		else if (type.equals("ftFloat")) 
			res =  (Float)getFloat(columnIndex);
		else if (type.equals("ftDouble"))
			res =  (Double)getDouble(columnIndex);
		else if (type.equals("ftDate")) 
			res =  (Date)getDate(columnIndex);
		else if (type.equals("ftDateTime"))
			res =  (Timestamp)getTimestamp(columnIndex);
		else if (type.equals("ftVarchar")) 
			res =  (String)getString(columnIndex);
		else if (type.equals("ftBlob")) 	
			res =  (String)getString(columnIndex);
		
		return (isNull) ? null : res;
	}

	
	private static int getCharLength(char[] value) {
		int size = value.length;
		for (int i = 0; i < size; i++) {
			if (value[i] == '\0')
				return i;
		}
		
		return size;
	}

	public static String nullessStr(char[] chars) {
		int size = getCharLength(chars);
		return new String(chars, 0, size);
	}

	/*
	 * private static int getCharLength(char[] value) { int size = value.length;
	 * for (int i = 0; i < size; i++) { if (value[i] == '\0') return i; } return
	 * size; }
	 */

	
	boolean memFlag = false;
	Runtime runtime;
	int mb = 1024 * 1024;
	boolean endOfFetch = false;

	public void showMemoryStat() {
		if (!memFlag)
			runtime = Runtime.getRuntime();
		long allocatedMemory = (runtime.totalMemory() - runtime.freeMemory());
		long presumableFreeMemory = runtime.maxMemory() - allocatedMemory;
		System.out.println("presumableFreeMemory = " + presumableFreeMemory
				/ mb + "mb allocatedMemory = " + allocatedMemory / mb + "mb");
	}

	/*
	 * public Boolean getData() throws IOException, SqreamRunTimeException,
	 * SQLException { SqreamLog.writeInfo("");
	 * GeneralUtils.writeln("SQResultSet.GetData(fetch..)"); if (Status ==
	 * RS_STAT.CLOSE) return false;
	 * 
	 * // QueryType[] resType = Metadata; try {
	 * GeneralUtils.writeln("SQResultSet TRY FETCH:");
	 * SQClientHandle.showClientInfo(Client); if (!MetaDataResultSet) {
	 * Rd.rows.clear(); Rd = new ResultData(); }
	 * 
	 * // long rows = Client.fetch(Resp); long rows =
	 * Client.fetch(Client.getQueryTypeOutSaved(), Rd); //
	 * long rows=Client.fetch(resType,Rd);
	 * GeneralUtils.writeln("SQResultSet TRY FETCH OK, rows = " +
	 * Rd.dataSizes.rows); // TODO razi check if (Rd == null) {
	 * GeneralUtils.writeln("next.rd == null"); throw new
	 * SQLException("ResultData is NULL"); }
	 * 
	 * if (rows == 0) { Status = RS_STAT.CLOSE; return false; } // Rd.rtype =
	 * resType; Rd.rtype = Client.getQueryTypeOutSaved(); //
	 * Rd.rebuildByteArrayPointers(); // a helper to the "Rd.getRow(i)" //
	 * Rd.rebuildByteArrayPointers4Or(); // the old server packet // support
	 * (data+null order)
	 * 
	 * // for(int i=0 ;i<Rd.rowsNum; i++ ){ // rows.add( Rd.getRow(i)); // }
	 * CurrentPos = -1; return true; } catch (SQLException e) {
	 * e.printStackTrace(); throw new SQLException(e); } }
	 */

	@Override
	public boolean next() throws SQLException {

		if (Empty) 
			return false;
		try {
			 boolean nextResult = this.Client.next();
			 return nextResult;
		} catch (Exception e2) {
			//e2.printStackTrace();
			try {
				if (Client!= null && Client.is_open() && Client.is_open_statement()) 
					Client.close();
			} catch (IOException | ConnException | ScriptException e) {
				e.printStackTrace();
				throw new SQLException(e.getMessage());
			}
			e2.printStackTrace();
			throw new SQLException(e2.getMessage());
	   } 
	}

	@Override
	public boolean wasNull() throws SQLException {
		return isNull;
	}
	
	@Override
	public int findColumn(String columnLabel) throws SQLException {
		this.baseUsageError();
		return 0;
	}
	
	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearWarnings() throws SQLException {

		this.baseUsageError(); 
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getCursorName() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isFirst() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isLast() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void beforeFirst() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public void afterLast() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean first() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean last() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		this.baseUsageError();
		return false;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean previous() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchDirection() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchSize() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getType() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getConcurrency() throws SQLException {
		return CONCUR_READ_ONLY;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowInserted() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}	

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		this.baseUsageError();

	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void insertRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void deleteRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void refreshRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getHoldability() throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return is_closed;
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		this.baseUsageError();
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		this.baseUsageError();
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		this.baseUsageError();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}
