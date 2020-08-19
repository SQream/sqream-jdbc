package com.sqream.jdbc.connector;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicBoolean;

public interface Connector extends AutoCloseable {

    int connect(String _database, String _user, String _password, String _service) throws ConnException;

    int execute(String statement) throws ConnException;

    int execute(String statement, int _chunk_size) throws ConnException;

    boolean next() throws ConnException;

    void close() throws ConnException;

    boolean closeConnection() throws ConnException;

    // -o-o-o-o-o    By index -o-o-o-o-o
    Boolean getBoolean(int col_num);

    Byte getUbyte(int col_num) throws ConnException;  // .get().toUnsignedInt()  -->  to allow values between 127-255

    Short getShort(int col_num) throws ConnException;

    Integer getInt(int col_num) throws ConnException;

    Long getLong(int col_num) throws ConnException;

    Float getFloat(int col_num) throws ConnException;

    Double getDouble(int col_num) throws ConnException;

    String getVarchar(int col_num) throws ConnException;

    String getNvarchar(int col_num) throws ConnException;

    Date getDate(int col_num, ZoneId zone) throws ConnException;

    Timestamp getDatetime(int col_num, ZoneId zone) throws ConnException;

    Date getDate(int col_num) throws ConnException;

    Timestamp getDatetime(int col_num) throws ConnException;

    // -o-o-o-o-o  By column name -o-o-o-o-o
    Boolean getBoolean(String col_name) throws ConnException;

    Byte getUbyte(String col_name) throws ConnException;

    Short getShort(String col_name) throws ConnException;

    Integer getInt(String col_name) throws ConnException;

    Long getLong(String col_name) throws ConnException;

    Float getFloat(String col_name) throws ConnException;

    Double getDouble(String col_name) throws ConnException;

    String getVarchar(String col_name) throws ConnException;

    String getNvarchar(String col_name) throws ConnException;

    Date getDate(String col_name) throws ConnException;

    Date getDate(String col_name, ZoneId zone) throws ConnException;

    Timestamp getDatetime(String col_name) throws ConnException;

    Timestamp getDatetime(String col_name, ZoneId zone) throws ConnException;

    int getTimeout();

    boolean setBoolean(int col_num, Boolean value) throws ConnException;

    boolean setUbyte(int col_num, Byte value) throws ConnException;

    boolean setShort(int col_num, Short value) throws ConnException;

    boolean setInt(int col_num, Integer value) throws ConnException;

    boolean setLong(int col_num, Long value) throws ConnException;

    boolean setFloat(int col_num, Float value) throws ConnException;

    boolean setDouble(int col_num, Double value) throws ConnException;

    boolean setVarchar(int col_num, String value) throws ConnException;

    boolean setNvarchar(int col_num, String value) throws ConnException;

    boolean setDate(int col_num, Date date, ZoneId zone) throws ConnException;

    boolean setDatetime(int col_num, Timestamp ts, ZoneId zone) throws ConnException;

    boolean setDate(int col_num, Date value) throws ConnException;

    boolean setDatetime(int col_num, Timestamp value) throws ConnException;

    void setTimeout(int seconds);

    int getStatementId();

    String getQueryType();

    int getRowLength();

    String getColName(int col_num) throws ConnException;

    String getColType(int col_num) throws ConnException;

    String getColType(String col_name) throws ConnException;

    int getColSize(int col_num) throws ConnException;

    boolean isColNullable(int col_num) throws ConnException;

    boolean isOpenStatement();

    boolean isOpen();

    AtomicBoolean checkCancelStatement();

    void setOpenStatement(boolean openStatement);

    boolean setFetchLimit(int _fetch_limit) throws ConnException;

    int getFetchLimit();

    void setFetchSize(int fetchSize);

    int getFetchSize();
}
