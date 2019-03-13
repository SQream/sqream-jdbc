package com.sqream.jdbc;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import com.sqream.jdbc.ColumnMetadata;

import com.sqream.connector.Connector;
import com.sqream.connector.Connector.ConnException;


public class SQResultSetMetaData implements ResultSetMetaData {

	/**
	 * @param args
	 */

	Connector client = null;
	ColumnMetadata[] meta;
	String db_name;
	int row_length;
	
	static void print(Object printable) {
		System.out.println(printable);
	}
	
	public SQResultSetMetaData(Connector _client, String catalog) throws IOException, SQLException, ConnException {

		client = _client;
		// Fill up meta in a loop using api stuff
		db_name = catalog;
		row_length = client.get_row_length();
		meta = new ColumnMetadata[row_length];
		for (int idx = 0; idx < row_length; idx++) {
			meta[idx] = new ColumnMetadata(client.get_col_name(idx +1), client.get_col_type(idx +1), client.get_col_size(idx +1), client.is_col_nullable(idx +1));
		}
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
	public String getCatalogName(int column) throws SQLException {
		// TODO Auto-generated method stub

		// return MetaData[column].name;
		return db_name; // hard-coded for dotan,in future, do it right.
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		// TODO Auto-generated method stub

		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getColumnCount() throws SQLException {

		return meta.length;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		System.err.println("YYYYYYYYYYY = " + meta[column-1].type.tid.toString());
		return  meta[column-1].type.size; 
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
		/*
		public int getPrecision(int column) throws SQLException {

			Integer size = isSelect()? Integer.valueOf(QueryTypeOut[column - 1].type[1]):Integer.valueOf(QueryTypeIn[column - 1].type[1]);
			String type= isSelect()? QueryTypeOut[column - 1].type[0]:QueryTypeIn[column - 1].type[0];
			return type.equals("ftBlob") ? size / 4: size;

		}  */
		
		
		
		return meta[column-1].type.tid.toString().equals("NVarchar") ? meta[column-1].type.size / 4: meta[column-1].type.size;

	}

	@Override
	public int getScale(int column) throws SQLException {
		
//		return meta[column-1].scale;
		return 0;
	}

	@Override
	public String getSchemaName(int column) throws SQLException {

		// TODO Auto-generated method stub
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		// TODO Auto-generated method stub

		return meta[column-1].name;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public boolean isCurrency(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
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
	public boolean isSearchable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}      

	@Override
	public boolean isSigned(int column) throws SQLException {
	    String col_type = meta[column-1].type.tid.toString(); 
        return Arrays.asList("Smallint", "Int", "Bigint", "Real", "Float").contains(col_type);
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
