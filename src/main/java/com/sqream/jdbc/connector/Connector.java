package com.sqream.jdbc.connector;

import javax.script.ScriptException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicBoolean;

public interface Connector extends AutoCloseable {

    int connect(String _database, String _user, String _password, String _service) throws IOException, ScriptException, ConnException;

    int execute(String statement) throws IOException, ScriptException, ConnException, KeyManagementException, NoSuchAlgorithmException;

    int execute(String statement, int _chunk_size) throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException;

    boolean next() throws ConnException, IOException, ScriptException;

    void close() throws IOException, ScriptException, ConnException;

    boolean closeConnection() throws IOException, ScriptException, ConnException;

    // -o-o-o-o-o    By index -o-o-o-o-o
    Boolean getBoolean(int col_num) throws ConnException;

    Byte getUbyte(int col_num) throws ConnException;  // .get().toUnsignedInt()  -->  to allow values between 127-255

    Short getShort(int col_num) throws ConnException;

    Integer getInt(int col_num) throws ConnException;

    Long getLong(int col_num) throws ConnException;

    Float getFloat(int col_num) throws ConnException;

    Double getDouble(int col_num) throws ConnException;

    String getVarchar(int col_num) throws ConnException, UnsupportedEncodingException;

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

    String getVarchar(String col_name) throws ConnException, UnsupportedEncodingException;

    String getNvarchar(String col_name) throws ConnException;

    Date getDate(String col_name) throws ConnException;

    Date getDate(String col_name, ZoneId zone) throws ConnException;

    Timestamp getDatetime(String col_name) throws ConnException;

    Timestamp getDatetime(String col_name, ZoneId zone) throws ConnException;

    boolean setBoolean(int col_num, Boolean value) throws ConnException;

    boolean setUbyte(int col_num, Byte value) throws ConnException;

    boolean setShort(int col_num, Short value) throws ConnException;

    boolean setInt(int col_num, Integer value) throws ConnException;

    boolean setLong(int col_num, Long value) throws ConnException;

    boolean setFloat(int col_num, Float value) throws ConnException;

    boolean setDouble(int col_num, Double value) throws ConnException;

    boolean setVarchar(int col_num, String value) throws ConnException, UnsupportedEncodingException;

    boolean setNvarchar(int col_num, String value) throws ConnException, UnsupportedEncodingException;

    boolean setDate(int col_num, Date date, ZoneId zone) throws ConnException, UnsupportedEncodingException;

    boolean setDatetime(int col_num, Timestamp ts, ZoneId zone) throws ConnException, UnsupportedEncodingException;

    boolean setDate(int col_num, Date value) throws ConnException, UnsupportedEncodingException;

    boolean setDatetime(int col_num, Timestamp value) throws ConnException, UnsupportedEncodingException;

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
}
