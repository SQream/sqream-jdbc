package com.sqream.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;

import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnException;


public class SQResultSetMetaData implements ResultSetMetaData {

	/**
	 * @param args
	 */

	Connector client;
	ColumnMetadata[] meta;
	String dbName;
	int rowLength;
	
	static void print(Object printable) {
		System.out.println(printable);
	}
	
	SQResultSetMetaData(Connector client, String catalog) throws ConnException {
		this.client = client;
		// Fill up meta in a loop using api stuff
		dbName = catalog;
		rowLength = client.getRowLength();
		meta = new ColumnMetadata[rowLength];
		for (int idx = 0; idx < rowLength; idx++) {
			meta[idx] = new ColumnMetadata(client.getColName(idx +1), client.get_col_type(idx +1), client.get_col_size(idx +1), client.is_col_nullable(idx +1));
		}
	}	
	
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		// TODO Auto-generated method stub
		// return MetaData[column].name;
		return dbName; // hard-coded for dotan,in future, do it right.
	}
	
	@Override
	public int getColumnCount() throws SQLException {

		return meta.length;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		
		return meta[column-1].type.tid.toString().equals("NVarchar") ? meta[column-1].type.size / 4: meta[column-1].type.size;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		// TODO Auto-generated method stub
		return meta[column-1].name;
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return meta[column-1].name;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		
		return meta[column-1].type.tid.getValue();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		
		return meta[column-1].type.tid.toString();
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		
		return meta[column-1].type.tid.toString().equals("NVarchar") ? meta[column-1].type.size / 4: meta[column-1].type.size;
	}

	@Override
	public int getScale(int column) throws SQLException {
		
//		return meta[column-1].scale;
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		// TODO Auto-generated method stub

		return meta[column-1].name;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
		//throw new SQLFeatureNotSupportedException();
	}
	
	
	@Override
	public int isNullable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return meta[column-1].isNul ? 1 : 0;
		//return 0;
	}
	
	/*
	 Retrieves whether this Connection object is in read-only mode.

	Returns:
	    true if this Connection object is read-only; false otherwise
	 */
	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return false;
	}
	
	@Override
	public boolean isSigned(int column) throws SQLException {
	    String col_type = meta[column-1].type.tid.toString(); 
        return Arrays.asList("Smallint", "Int", "Bigint", "Real", "Float").contains(col_type);
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("unwrap in SQResultSetMetaData");
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("getColumnClassName in SQResultSetMetaData");
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("getSchemaName in SQResultSetMetaData");
	}
	
	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("isCaseSensitive in SQResultSetMetaData");
	}
	
	@Override
	public boolean isCurrency(int column) throws SQLException {
		// Indicates whether the designated column is a cash value.
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("isDefinitelyWritable in SQResultSetMetaData");
	}
	
	@Override
	public boolean isSearchable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("isSearchable in SQResultSetMetaData");
	}      

	@Override
	public boolean isWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("isWritable in SQResultSetMetaData");
	}

}
