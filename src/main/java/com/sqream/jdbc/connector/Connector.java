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

public interface Connector {

    int connect(String _database, String _user, String _password, String _service) throws IOException, ScriptException, ConnectorImpl.ConnException;

    int execute(String statement) throws IOException, ScriptException, ConnectorImpl.ConnException, KeyManagementException, NoSuchAlgorithmException;

    int execute(String statement, int _chunk_size) throws IOException, ScriptException, ConnectorImpl.ConnException, NoSuchAlgorithmException, KeyManagementException;

    boolean next() throws ConnectorImpl.ConnException, IOException, ScriptException;

    Boolean close() throws IOException, ScriptException, ConnectorImpl.ConnException;

    boolean closeConnection() throws IOException, ScriptException, ConnectorImpl.ConnException;

    // -o-o-o-o-o    By index -o-o-o-o-o
    Boolean getBoolean(int col_num) throws ConnectorImpl.ConnException;

    Byte get_ubyte(int col_num) throws ConnectorImpl.ConnException;  // .get().toUnsignedInt()  -->  to allow values between 127-255

    Short get_short(int col_num) throws ConnectorImpl.ConnException;

    Integer get_int(int col_num) throws ConnectorImpl.ConnException;

    Long get_long(int col_num) throws ConnectorImpl.ConnException;

    Float get_float(int col_num) throws ConnectorImpl.ConnException;

    Double get_double(int col_num) throws ConnectorImpl.ConnException;

    String get_varchar(int col_num) throws ConnectorImpl.ConnException, UnsupportedEncodingException;

    String get_nvarchar(int col_num) throws ConnectorImpl.ConnException;

    Date get_date(int col_num, ZoneId zone) throws ConnectorImpl.ConnException;

    Timestamp get_datetime(int col_num, ZoneId zone) throws ConnectorImpl.ConnException;

    Date get_date(int col_num) throws ConnectorImpl.ConnException;

    Timestamp get_datetime(int col_num) throws ConnectorImpl.ConnException;

    // -o-o-o-o-o  By column name -o-o-o-o-o
    Boolean getBoolean(String col_name) throws ConnectorImpl.ConnException;

    Byte get_ubyte(String col_name) throws ConnectorImpl.ConnException;

    Short get_short(String col_name) throws ConnectorImpl.ConnException;

    Integer get_int(String col_name) throws ConnectorImpl.ConnException;

    Long get_long(String col_name) throws ConnectorImpl.ConnException;

    Float get_float(String col_name) throws ConnectorImpl.ConnException;

    Double get_double(String col_name) throws ConnectorImpl.ConnException;

    String get_varchar(String col_name) throws ConnectorImpl.ConnException, UnsupportedEncodingException;

    String get_nvarchar(String col_name) throws ConnectorImpl.ConnException;

    Date get_date(String col_name) throws ConnectorImpl.ConnException;

    Date get_date(String col_name, ZoneId zone) throws ConnectorImpl.ConnException;

    Timestamp get_datetime(String col_name) throws ConnectorImpl.ConnException;

    Timestamp get_datetime(String col_name, ZoneId zone) throws ConnectorImpl.ConnException;

    int getStatementId();

    String getQueryType();

    int getRowLength();

    String getColName(int col_num) throws ConnectorImpl.ConnException;

    String get_col_type(int col_num) throws ConnectorImpl.ConnException;

    String get_col_type(String col_name) throws ConnectorImpl.ConnException;

    int get_col_size(int col_num) throws ConnectorImpl.ConnException;

    boolean is_col_nullable(int col_num) throws ConnectorImpl.ConnException;

    boolean isOpenStatement();

    boolean isOpen();

    AtomicBoolean checkCancelStatement();

    void setOpenStatement(boolean openStatement);

    boolean setFetchLimit(int _fetch_limit) throws ConnectorImpl.ConnException;

    int getFetchLimit();
}
