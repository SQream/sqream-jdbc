package com.sqream.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.enums.SqreamTypeId;


public class SQResultSetMetaData implements ResultSetMetaData {
	private static final Logger LOGGER = Logger.getLogger(SQResultSetMetaData.class.getName());

	private static final Map<SqreamTypeId, Integer> displaySizeMap = new HashMap<>();

	static {
		displaySizeMap.put(SqreamTypeId.Bool, 1);
		displaySizeMap.put(SqreamTypeId.Tinyint, 3);
		displaySizeMap.put(SqreamTypeId.Smallint, 6);
		displaySizeMap.put(SqreamTypeId.Int, 11);
		displaySizeMap.put(SqreamTypeId.Bigint, 20);
		displaySizeMap.put(SqreamTypeId.Real, 10);
		displaySizeMap.put(SqreamTypeId.Float, 12);
		displaySizeMap.put(SqreamTypeId.Date, 10);
		displaySizeMap.put(SqreamTypeId.DateTime, 23);
		displaySizeMap.put(SqreamTypeId.NVarchar, Integer.MAX_VALUE);
	}

	Connector client;
	ColumnMetadata[] meta;
	String dbName;
	int rowLength;

	SQResultSetMetaData(Connector client, String catalog) throws ConnException {
		this.client = client;
		// Fill up meta in a loop using api stuff
		dbName = catalog;
		rowLength = client.getRowLength();
		meta = new ColumnMetadata[rowLength];
		for (int idx = 0; idx < rowLength; idx++) {
			meta[idx] = new ColumnMetadata(client.getColName(idx +1), client.getColType(idx +1), client.getColSize(idx +1), client.isColNullable(idx +1));
		}
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}] for interface=[{1}]", false, iface));
		return false;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}] for column=[{1}]", dbName, column));
		// return MetaData[column].name;
		return dbName; // hard-coded for dotan,in future, do it right.
	}

	@Override
	public int getColumnCount() throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", meta.length));
		return meta.length;
	}

	public int getColumnDisplaySize(int column) throws SQLException {
		int result;
		if ("Varchar".equals(meta[column-1].getTypeId().toString())) {
			result = meta[column - 1].getType().size;
		} else {
			result = displaySizeMap.get(meta[column-1].getTypeId());
		}
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", result));
		return result;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		String result = meta[column-1].getName();
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", result));
		return result;
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		String result = meta[column-1].getName();
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", result));
		return result;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		int result = meta[column-1].getTypeId().getValue();
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", result));
		return result;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		String result = meta[column-1].getTypeId().toString();
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", result));
		return result;
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		int result = meta[column-1].getTypeId().toString().equals("NVarchar") ? meta[column-1].getType().size / 4: meta[column-1].getType().size;
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", result));
		return result;
	}

	@Override
	public int getScale(int column) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", 0));
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		String result = meta[column-1].getName();
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", result));
		return result;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", false));
		return false;
	}


	@Override
	public int isNullable(int column) throws SQLException {
		int result = meta[column-1].isNull() ? 1 : 0;
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}]", result));
		return result;
	}

	/*
	 Retrieves whether this Connection object is in read-only mode.

	Returns:
	    true if this Connection object is read-only; false otherwise
	 */
	@Override
	public boolean isReadOnly(int column) throws SQLException {
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}] for column=[{1}]", false, column));
		return false;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
	    String col_type = meta[column-1].getTypeId().toString();
        boolean result = Arrays.asList("Smallint", "Int", "Bigint", "Real", "Float").contains(col_type);
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}] for column=[{1}]", result, column));
        return result;
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
		LOGGER.log(Level.FINE, MessageFormat.format("returns [{0}] for column=[{1}]", false, column));
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
