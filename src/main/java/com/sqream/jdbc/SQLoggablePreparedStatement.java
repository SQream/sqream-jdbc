package com.sqream.jdbc;


import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.Connector;
import com.sqream.jdbc.connector.ConnectorImpl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class SQLoggablePreparedStatement extends SQPreparedStatement implements PreparedStatement {
    private static final Logger LOGGER = Logger.getLogger(SQLoggablePreparedStatement.class.getName());

    protected SQLoggablePreparedStatement(String sql, ConnectionParams connParams) throws ConnException {
        super(sql, connParams);
    }

    @Override
    public void close() throws SQLException {
    	LOGGER.log(Level.FINE,"Close prepared statement");
        super.close();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        LOGGER.log(Level.FINE,"execute batch");
        return super.executeBatch();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        LOGGER.log(Level.INFO, "get ResultSet");
        return super.getResultSet();
    }

    @Override
    public void addBatch() throws SQLException {
        LOGGER.log(Level.FINEST, "add batch");
        super.addBatch();
    }

    @Override
    public boolean execute() {
        LOGGER.log(Level.FINE,"execute");
        return super.execute();
    }

    @Override
    public ResultSet executeQuery() {
        LOGGER.log(Level.FINE,"execute query");
        return super.executeQuery();
    }

    // set()
    // ----

    @Override
    public void setBoolean(int colNum, boolean value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setBoolean(colNum, value);
    }

    @Override
    public void setByte(int colNum, byte value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setByte(colNum, value);
    }

    @Override
    public void setShort(int colNum, short value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setShort(colNum, value);
    }

    @Override
    public void setInt(int colNum, int value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setInt(colNum, value);
    }

    @Override
    public void setLong(int colNum, long value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setLong(colNum, value);
    }

    @Override
    public void setFloat(int colNum, float value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setFloat(colNum, value);
    }

    @Override
    public void setDouble(int colNum, double value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setDouble(colNum, value);
    }

    @Override
    public void setDate(int colNum, Date date) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setDate(colNum, date);
    }

    @Override
    public void setDate(int colNum, Date date, Calendar cal) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}], Calendar=[{1}]", colNum, cal));
        super.setDate(colNum, date, cal);
    }

    @Override
    public void setTimestamp(int colNum, Timestamp datetime) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setTimestamp(colNum, datetime);
    }

    @Override
    public void setTimestamp(int colNum, Timestamp datetime, Calendar cal) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}], Calendar=[{1}]", colNum, cal));
    }

    @Override
    public void setString(int colNum, String value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setString(colNum, value);
    }

    @Override
    public void setNString(int colNum, String value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setNString(colNum, value);
    }

    @Override
    public void setNull(int colNum, int value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setNull(colNum, value);
    }

    @Override
    public void setObject(int colNum, Object value) throws SQLException {
        LOGGER.log(Level.FINEST, () -> MessageFormat.format("ColNum=[{0}]", colNum));
        super.setObject(colNum, value);
    }


    public ResultSetMetaData getMetaData() throws SQLException {
        LOGGER.log(Level.FINEST, "get metadata");
        return super.getMetaData();
    }

    // ----------------

    @Override
    public void setFetchSize(int rows) throws SQLException {
       LOGGER.log(Level.FINE, MessageFormat.format("rows=[{0}]", rows));
       super.setFetchSize(rows);
    }

    @Override
    public boolean execute(String arg0) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("statement=[{0}]", arg0));
        return super.execute(arg0);
    }

    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}], arg1=[{1}]", arg0, arg1));
        return super.execute(arg0, arg1);
    }

    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}], arg1=[{1}]", arg0, arg1));
        return super.execute(arg0, arg1);
    }

    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}], arg1=[{1}]", arg0, arg1));
        return super.execute(arg0, arg1);
    }

    @Override
    public int executeUpdate(String arg0) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}]", arg0));
        return super.executeUpdate(arg0);
    }

    @Override
    public int executeUpdate(String arg0, int arg1) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}], arg1=[{1}]", arg0, arg1));
        return super.executeUpdate(arg0, arg1);
    }

    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}], arg1=[{1}]", arg0, arg1));
        return super.executeUpdate();
    }

    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}], arg1=[{1}]", arg0, arg1));
        return super.executeUpdate(arg0, arg1);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        LOGGER.log(Level.FINE, "get fetch direction");
        return super.getFetchDirection();
    }

    @Override
    public int getFetchSize() throws SQLException {
        LOGGER.log(Level.FINE, "get fetch size");
        return super.getFetchSize();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.getMaxFieldSize();
    }

    @Override
    public int getMaxRows() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.getMaxRows();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.getMoreResults();
    }

    @Override
    public boolean getMoreResults(int arg0) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}]", arg0));
        return super.getMoreResults();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.getQueryTimeout();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.getResultSetConcurrency();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.getResultSetHoldability();
    }

    @Override
    public int getResultSetType() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.getResultSetType();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.getUpdateCount();
    }

    @Override
    public boolean isClosed() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.isClosed();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.isCloseOnCompletion();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.isPoolable();
    }

    @Override
    public void cancel() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        super.cancel();
    }

    @Override
    public void clearBatch() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        super.clearBatch();
    }

    @Override
    public void clearWarnings() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        super.clearWarnings();
    }

    @Override
    public void setNull(int arg0, int arg1, String arg2) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}], arg1=[{1}], arg2=[{2}]", arg0, arg1, arg2));
        super.setNull(arg0, arg1, arg2);
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("arg0=[{0}]", arg0));
        return super.isWrapperFor(arg0);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        LOGGER.log(Level.FINE, "called");
        return super.getParameterMetaData();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("seconds=[{0}]", seconds));
        super.setQueryTimeout(seconds);
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        LOGGER.log(Level.FINE, MessageFormat.format("maxRows=[{0}]", maxRows));
        super.setMaxRows(maxRows);
    }
}
