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

    int connect(String _database, String _user, String _password, String _service) throws IOException, ScriptException, ConnException;

    int execute(String statement) throws IOException, ScriptException, ConnException, KeyManagementException, NoSuchAlgorithmException;

    int execute(String statement, int _chunk_size) throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException;

    boolean next() throws ConnException, IOException, ScriptException;

    Boolean close() throws IOException, ScriptException, ConnException;

    boolean closeConnection() throws IOException, ScriptException, ConnException;

    // -o-o-o-o-o    By index -o-o-o-o-o
    Boolean getBoolean(int col_num) throws ConnException;

    Byte get_ubyte(int col_num) throws ConnException;  // .get().toUnsignedInt()  -->  to allow values between 127-255

    Short get_short(int col_num) throws ConnException;

    Integer get_int(int col_num) throws ConnException;

    Long get_long(int col_num) throws ConnException;

    Float get_float(int col_num) throws ConnException;

    Double get_double(int col_num) throws ConnException;

    String get_varchar(int col_num) throws ConnException, UnsupportedEncodingException;

    String get_nvarchar(int col_num) throws ConnException;

    Date get_date(int col_num, ZoneId zone) throws ConnException;

    Timestamp get_datetime(int col_num, ZoneId zone) throws ConnException;

    Date get_date(int col_num) throws ConnException;

    Timestamp get_datetime(int col_num) throws ConnException;

    // -o-o-o-o-o  By column name -o-o-o-o-o
    Boolean getBoolean(String col_name) throws ConnException;

    Byte get_ubyte(String col_name) throws ConnException;

    Short get_short(String col_name) throws ConnException;

    Integer get_int(String col_name) throws ConnException;

    Long get_long(String col_name) throws ConnException;

    Float get_float(String col_name) throws ConnException;

    Double get_double(String col_name) throws ConnException;

    String get_varchar(String col_name) throws ConnException, UnsupportedEncodingException;

    String get_nvarchar(String col_name) throws ConnException;

    Date get_date(String col_name) throws ConnException;

    Date get_date(String col_name, ZoneId zone) throws ConnException;

    Timestamp get_datetime(String col_name) throws ConnException;

    Timestamp get_datetime(String col_name, ZoneId zone) throws ConnException;

    boolean set_boolean(int col_num, Boolean value) throws ConnException;

    boolean set_ubyte(int col_num, Byte value) throws ConnException;

    boolean set_short(int col_num, Short value) throws ConnException;

    boolean set_int(int col_num, Integer value) throws ConnException;

    boolean set_long(int col_num, Long value) throws ConnException;

    boolean set_float(int col_num, Float value) throws ConnException;

    boolean set_double(int col_num, Double value) throws ConnException;

    boolean set_varchar(int col_num, String value) throws ConnException, UnsupportedEncodingException;

    boolean set_nvarchar(int col_num, String value) throws ConnException, UnsupportedEncodingException;

    boolean set_date(int col_num, Date date, ZoneId zone) throws ConnException, UnsupportedEncodingException;

    boolean set_datetime(int col_num, Timestamp ts, ZoneId zone) throws ConnException, UnsupportedEncodingException;

    boolean set_date(int col_num, Date value) throws ConnException, UnsupportedEncodingException;

    boolean set_datetime(int col_num, Timestamp value) throws ConnException, UnsupportedEncodingException;

    int getStatementId();

    String getQueryType();

    int getRowLength();

    String getColName(int col_num) throws ConnException;

    String get_col_type(int col_num) throws ConnException;

    String get_col_type(String col_name) throws ConnException;

    int get_col_size(int col_num) throws ConnException;

    boolean is_col_nullable(int col_num) throws ConnException;

    boolean isOpenStatement();

    boolean isOpen();

    AtomicBoolean checkCancelStatement();

    void setOpenStatement(boolean openStatement);

    boolean setFetchLimit(int _fetch_limit) throws ConnException;

    int getFetchLimit();
}
