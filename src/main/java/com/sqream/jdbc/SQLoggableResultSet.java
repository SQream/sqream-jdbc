package com.sqream.jdbc;

import com.sqream.jdbc.connector.Connector;

import java.sql.*;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

class SQLoggableResultSet extends SQResultSet implements ResultSet {
	private static final Logger LOGGER = Logger.getLogger(SQLoggableResultSet.class.getName());

	protected SQLoggableResultSet(Connector client, String catalog) {
		super(client, catalog);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		LOGGER.log(Level.FINE, "get metadata");
		return super.getMetaData();
	}

	@Override
	public boolean next() throws SQLException {
		boolean result = super.next();
		LOGGER.log(Level.FINEST, String.format("ResultSet has next row [%s]", result));
		return result;
	}

	@Override
	public boolean wasNull() {
		boolean result = super.wasNull();
		LOGGER.log(Level.FINEST, String.valueOf(result));
		return result;
	}

	@Override
	public int getConcurrency() {
		int result = super.getConcurrency();
		LOGGER.log(Level.FINE, String.valueOf(result));
		return result;
	}

	@Override
	public boolean isClosed() {
		boolean result = super.isClosed();
		LOGGER.log(Level.FINE, String.valueOf(result));
		return result;
	}

	@Override
	public void close() throws SQLException {
		LOGGER.log(Level.FINE, "Close ResultSet");
		super.close();
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getBoolean(columnLabel);
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getBoolean(columnIndex);
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getByte(columnLabel);
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getByte(columnIndex);
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getShort(columnLabel);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getShort(columnIndex);
	}


	@Override
	public int getInt(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getInt(columnLabel);
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getInt(columnIndex);
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getLong(columnLabel);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getLong(columnIndex);
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getFloat(columnLabel);
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getFloat(columnIndex);
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getDouble(columnLabel);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getDouble(columnIndex);
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getDate(columnIndex);
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getDate(columnLabel);
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s], calendar [%s]", columnIndex, cal));
		return super.getDate(columnIndex, cal);
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s], calendar [%s]", columnLabel, cal));
		return super.getDate(columnLabel, cal);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getTimestamp(columnLabel);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getTimestamp(columnIndex);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) {
		LOGGER.log(Level.FINEST, String.format("column index [%s], calendar [%s]", columnIndex, cal));
		return super.getTimestamp(columnIndex, cal);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s], calendar [%s]", columnLabel, cal));
		return super.getTimestamp(columnLabel, cal);
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getString(columnLabel);
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getString(columnIndex);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column label [%s]", columnLabel));
		return super.getObject(columnLabel);
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		LOGGER.log(Level.FINEST, String.format("column index [%s]", columnIndex));
		return super.getObject(columnIndex);
	}
}
